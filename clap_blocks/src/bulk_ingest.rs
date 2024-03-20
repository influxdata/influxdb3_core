//! CLI config for the router to enable bulk ingest APIs

use ed25519_dalek::{
    pkcs8::{DecodePrivateKey, DecodePublicKey},
    SigningKey, VerifyingKey,
};
use snafu::{ResultExt, Snafu};
use std::{fs, io, path::PathBuf};

/// CLI config for bulk ingest.
#[derive(Debug, Clone, Default, clap::Parser)]
pub struct BulkIngestConfig {
    /// Private signing key used for Parquet metadata returned from the `NewParquetMetadata` gRPC
    /// API to prevent tampering/corruption of Parquet metadata provided by IOx to the process
    /// preparing Parquet files for bulk ingest.
    ///
    /// This is a path to an Ed25519 private key file generated by OpenSSL with the command:
    /// `openssl genpkey -algorithm ed25519 -out private-key-filename.pem`
    ///
    /// The public key used to verify signatures will be derived from this private key. Additional
    /// public verification keys can be specified with
    /// `-bulk-ingest-additional-verification-key-files` to support key rotation.
    ///
    /// If not specified, the `NewParquetMetadata` gRPC API will return unimplemented.
    #[clap(
        long = "bulk-ingest-metadata-signing-key-file",
        env = "INFLUXDB_IOX_BULK_INGEST_METADATA_SIGNING_KEY_FILE"
    )]
    metadata_signing_key_file: Option<PathBuf>,

    /// When in the process of rotating keys, specify paths to files containing public verification
    /// keys of previously used private signing keys used for signing metadata in the past.
    ///
    /// These files can be derived from private key files with this OpenSSL command:
    /// `openssl pkey -in private-key-filename.pem -pubout -out public-key-filename.pem`
    ///
    /// Example: "public-key-1.pem,public-key-2.pem"
    ///
    /// If verification of the metadata signature fails with the current public key derived from
    /// the current signing key, these verification keys will be tested in order to allow older
    /// signatures generated with the old key to still be validated. For best performance of
    /// signature verification, specify the additional verification keys in order of most likely
    /// candidates first (probably most recently used first).
    ///
    /// If no additional verification keys are specified, only the verification key associated with
    /// the current metadata signing key will be used to validate signatures.
    #[clap(
        long = "bulk-ingest-additional-verification-key-files",
        env = "INFLUXDB_IOX_BULK_INGEST_ADDITIONAL_VERIFICATION_KEY_FILES",
        required = false,
        num_args=1..,
        value_delimiter = ',',
    )]
    additional_verification_key_files: Vec<PathBuf>,

    /// Rather than using whatever object store configuration may have been specified as a source
    /// of presigned upload URLs for bulk ingest, use a mock implementation that returns an upload
    /// URL value that can be inspected but not used.
    ///
    /// Only useful for testing bulk ingest without setting up S3! Do not use this in production!
    #[clap(
        hide = true,
        long = "bulk-ingest-use-mock-presigned-url-signer",
        env = "INFLUXDB_IOX_BULK_INGEST_USE_MOCK_PRESIGNED_URL_SIGNER"
    )]
    pub use_mock_presigned_url_signer: Option<String>,
}

impl BulkIngestConfig {
    /// Constructor for bulk ingest configuration.
    pub fn new(
        metadata_signing_key_file: Option<PathBuf>,
        additional_verification_key_files: Vec<PathBuf>,
        use_mock_presigned_url_signer: Option<String>,
    ) -> Self {
        Self {
            metadata_signing_key_file,
            additional_verification_key_files,
            use_mock_presigned_url_signer,
        }
    }
}

impl TryFrom<&BulkIngestConfig> for Option<BulkIngestKeys> {
    type Error = BulkIngestConfigError;

    fn try_from(config: &BulkIngestConfig) -> Result<Self, Self::Error> {
        config
            .metadata_signing_key_file
            .as_ref()
            .map(|signing_key_file| {
                let signing_key: SigningKey = fs::read_to_string(signing_key_file)
                    .context(ReadingSigningKeyFileSnafu {
                        filename: &signing_key_file,
                    })
                    .and_then(|file_contents| {
                        DecodePrivateKey::from_pkcs8_pem(&file_contents).context(
                            DecodingSigningKeySnafu {
                                filename: signing_key_file,
                            },
                        )
                    })?;

                let additional_verifying_keys: Vec<_> = config
                    .additional_verification_key_files
                    .iter()
                    .map(|verification_key_file| {
                        fs::read_to_string(verification_key_file)
                            .context(ReadingVerifyingKeyFileSnafu {
                                filename: &verification_key_file,
                            })
                            .and_then(|file_contents| {
                                DecodePublicKey::from_public_key_pem(&file_contents).context(
                                    DecodingVerifyingKeySnafu {
                                        filename: verification_key_file,
                                    },
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(BulkIngestKeys {
                    signing_key,
                    additional_verifying_keys,
                })
            })
            .transpose()
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum BulkIngestConfigError {
    #[snafu(display("Could not read signing key from {}: {source}", filename.display()))]
    ReadingSigningKeyFile {
        filename: PathBuf,
        source: io::Error,
    },

    #[snafu(display("Could not decode signing key from {}: {source}", filename.display()))]
    DecodingSigningKey {
        filename: PathBuf,
        source: ed25519_dalek::pkcs8::Error,
    },

    #[snafu(display("Could not read verifying key from {}: {source}", filename.display()))]
    ReadingVerifyingKeyFile {
        filename: PathBuf,
        source: io::Error,
    },

    #[snafu(display("Could not decode verifying key from {}: {source}", filename.display()))]
    DecodingVerifyingKey {
        filename: PathBuf,
        source: ed25519_dalek::pkcs8::spki::Error,
    },
}

/// Key values extracted from the files specified to the CLI. To get an instance, first create a
/// `BulkIngestConfig`, then call `try_from` to get a `Result` containing an
/// `Option<BulkIngestKeys>` where the `Option` will be `Some` if the `BulkIngestConfig`'s
/// `metadata_signing_key_file` value is `Some`.
///
/// If any filenames specified anywhere in the `BulkIngestConfig` can't be read or don't contain
/// valid key values, the `try_from` implementation will return an error.
#[derive(Debug)]
pub struct BulkIngestKeys {
    /// The parsed private signing key value contained in the file specified to
    /// `--bulk-ingest-metadata-signing-key-file`.
    pub signing_key: SigningKey,

    /// If any files were specified in `--bulk-ingest-additional-verification-key-files`, this list
    /// will contain their parsed public verification key values.
    pub additional_verifying_keys: Vec<VerifyingKey>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::process::Command;
    use test_helpers::{assert_contains, make_temp_file, tmp_dir};

    #[test]
    fn missing_signing_key_param() {
        // No signing key file -> no keys
        let config = BulkIngestConfig::try_parse_from(["something"]).unwrap();
        let keys: Option<BulkIngestKeys> = (&config).try_into().unwrap();
        assert!(keys.is_none(), "expected None, got: {:?}", keys);

        // Even if there are additional verification key files; no signing key file means no keys
        let config = BulkIngestConfig::try_parse_from([
            "something",
            "--bulk-ingest-additional-verification-key-files",
            "some-public-key-filename.pem",
        ])
        .unwrap();
        let keys: Option<BulkIngestKeys> = (&config).try_into().unwrap();
        assert!(keys.is_none(), "expected None, got: {:?}", keys);
    }

    #[test]
    fn signing_key_file_not_found() {
        let nonexistent_filename = "do-not-create-a-file-with-this-name-or-this-test-will-fail";
        let config = BulkIngestConfig::try_parse_from([
            "something",
            "--bulk-ingest-metadata-signing-key-file",
            nonexistent_filename,
        ])
        .unwrap();

        let keys: Result<Option<BulkIngestKeys>, _> = (&config).try_into();
        let err = keys.unwrap_err();
        assert_contains!(
            err.to_string(),
            format!("Could not read signing key from {nonexistent_filename}")
        );
    }

    #[test]
    fn signing_key_file_contents_invalid() {
        let signing_key_file = make_temp_file("not a valid signing key");
        let signing_key_filename = signing_key_file.path().display().to_string();

        let config = BulkIngestConfig::try_parse_from([
            "something",
            "--bulk-ingest-metadata-signing-key-file",
            &signing_key_filename,
        ])
        .unwrap();

        let keys: Result<Option<BulkIngestKeys>, _> = (&config).try_into();
        let err = keys.unwrap_err();
        assert_contains!(
            err.to_string(),
            format!("Could not decode signing key from {signing_key_filename}")
        );
    }

    #[test]
    fn valid_signing_key_file_no_additional_key_files() {
        let tmp_dir = tmp_dir().unwrap();
        let signing_key_filename = tmp_dir
            .path()
            .join("test-private-key.pem")
            .display()
            .to_string();
        Command::new("openssl")
            .arg("genpkey")
            .arg("-algorithm")
            .arg("ed25519")
            .arg("-out")
            .arg(&signing_key_filename)
            .output()
            .unwrap();

        let config = BulkIngestConfig::try_parse_from([
            "something",
            "--bulk-ingest-metadata-signing-key-file",
            &signing_key_filename,
        ])
        .unwrap();

        let keys: Result<Option<BulkIngestKeys>, _> = (&config).try_into();
        let keys = keys.unwrap().unwrap();
        let additional_keys = keys.additional_verifying_keys;
        assert!(
            additional_keys.is_empty(),
            "expected additional keys to be empty, got {:?}",
            additional_keys
        );
    }
}
