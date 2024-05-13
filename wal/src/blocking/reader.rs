use crate::{FileTypeIdentifier, SegmentEntry, SegmentId, SegmentIdBytes, SequencedWalOp};
use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher;
use generated_types::influxdata::iox::wal::v1::WalOpBatch as ProtoWalOpBatch;
use prost::Message;
use snafu::prelude::*;
use snap::read::FrameDecoder;
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

/// A closed segment reader over an `R`, tracking the cumulative number of
/// encoded bytes read from the reader.
#[derive(Debug)]
pub(crate) struct ClosedSegmentReader<R> {
    reader: R,
    cumulative_bytes_read: u64,

    file_type_identifier: FileTypeIdentifier,
    segment_id: SegmentId,
}

impl ClosedSegmentReader<BufReader<File>> {
    /// A specialised constructor for a [`ClosedSegmentReader`] which wraps a
    /// buffered file reader for the file at `path`.
    pub(crate) fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path).context(UnableToOpenFileSnafu { path })?;
        let file = BufReader::new(file);
        Self::new(file)
    }
}

impl<R> ClosedSegmentReader<R>
where
    R: Read,
{
    /// Read the header of the WAL segment contained by R, returning its
    /// [`FileTypeIdentifier`] and the encoded [`SegmentId`].
    ///
    /// [`SegmentId`]: crate::SegmentId
    fn read_header(reader: &mut R) -> Result<(FileTypeIdentifier, SegmentIdBytes)> {
        Ok((
            read_array(reader).context(UnableToReadHeaderFieldSnafu {
                field_name: "file type identifier",
            })?,
            read_array(reader).context(UnableToReadHeaderFieldSnafu {
                field_name: "segment id bytes",
            })?,
        ))
    }

    /// Initialises a [`ClosedSegmentReader`] for synchronous reading of a
    /// closed WAL segment from the given [`Read`] implementation, reading and
    /// validating the segment file's header.
    pub(crate) fn new(mut reader: R) -> Result<Self> {
        // NOTE: The header must be read and validated.
        let (file_type_identifier, segment_id_bytes) = Self::read_header(&mut reader)?;

        Ok(Self {
            reader,
            cumulative_bytes_read: (file_type_identifier.len() + segment_id_bytes.len()) as u64,
            file_type_identifier,
            segment_id: SegmentId::from_bytes(segment_id_bytes),
        })
    }

    fn one_entry(&mut self) -> Result<Option<SegmentEntry>> {
        let expected_checksum = match self.reader.read_u32::<BigEndian>() {
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            other => other.context(UnableToReadChecksumSnafu)?,
        };

        let expected_len = self
            .reader
            .read_u32::<BigEndian>()
            .context(UnableToReadLengthSnafu)?
            .into();

        let compressed_read = self.reader.by_ref().take(expected_len);
        let hashing_read = CrcReader::new(compressed_read);
        let mut decompressing_read = FrameDecoder::new(hashing_read);

        let mut data = Vec::with_capacity(100);
        decompressing_read
            .read_to_end(&mut data)
            .context(UnableToReadDataSnafu)?;

        let (actual_compressed_len, actual_checksum) = decompressing_read.into_inner().checksum();

        // Track the size of the entry header and total amount of compressed
        // data successfully read so far by the reader. The header values are
        // tracked here to avoid continuously counting bytes read from a
        // corrupted segment where no further entries can be read.
        //
        // This accounting is done before checksum/length mismatch, if the data has still
        // been read in successfully.
        self.cumulative_bytes_read += 2 * std::mem::size_of::<u32>() as u64;
        self.cumulative_bytes_read += actual_compressed_len;

        ensure!(
            expected_len == actual_compressed_len,
            LengthMismatchSnafu {
                expected: expected_len,
                actual: actual_compressed_len
            }
        );

        ensure!(
            expected_checksum == actual_checksum,
            ChecksumMismatchSnafu {
                expected: expected_checksum,
                actual: actual_checksum
            }
        );

        Ok(Some(SegmentEntry { data }))
    }

    /// Reads the next entry in the closed WAL segment, decoding the contained
    /// [`SequencedWalOp`] batch within.
    ///
    /// This method returns [`None`] when all entries have been read from the
    /// segment.
    pub(crate) fn next_batch(&mut self) -> Result<Option<Vec<SequencedWalOp>>> {
        if let Some(entry) = self.one_entry()? {
            let decoded =
                ProtoWalOpBatch::decode(&*entry.data).context(UnableToDeserializeDataSnafu)?;

            let mut ops = Vec::with_capacity(decoded.ops.len());
            for op in decoded.ops {
                ops.push(op.try_into().context(InvalidMessageSnafu)?);
            }

            return Ok(Some(ops));
        }

        Ok(None)
    }

    /// Returns the cumulative count of encoded bytes successfully read from
    /// the underlying reader.
    pub(crate) fn bytes_read(&self) -> u64 {
        self.cumulative_bytes_read
    }

    /// Returns the [`FileTypeIdentifier`] read for this segment.
    pub(crate) fn file_type_identifier(&self) -> &FileTypeIdentifier {
        &self.file_type_identifier
    }

    /// Returns the WAL segment ID this reader is for.
    pub(crate) fn segment_id(&self) -> SegmentId {
        self.segment_id
    }
}

fn read_array<const N: usize, R: Read>(reader: &mut R) -> Result<[u8; N], ReadArrayError> {
    let mut data = [0u8; N];
    reader
        .read_exact(&mut data)
        .context(UnableToReadArraySnafu { length: N })?;
    Ok(data)
}

struct CrcReader<R> {
    inner: R,
    hasher: Hasher,
    bytes_seen: u64,
}

impl<R> CrcReader<R> {
    fn new(inner: R) -> Self {
        let hasher = Hasher::default();
        Self {
            inner,
            hasher,
            bytes_seen: 0,
        }
    }

    fn checksum(self) -> (u64, u32) {
        (self.bytes_seen, self.hasher.finalize())
    }
}

impl<R> Read for CrcReader<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.inner.read(buf)?;
        let len_u64 = u64::try_from(len).expect("Only designed to run on 32-bit systems or higher");

        self.bytes_seen += len_u64;
        self.hasher.update(&buf[..len]);
        Ok(len)
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    UnableToOpenFile {
        source: io::Error,
        path: PathBuf,
    },

    UnableToReadHeaderField {
        source: ReadArrayError,
        field_name: &'static str,
    },

    UnableToReadChecksum {
        source: io::Error,
    },

    UnableToReadLength {
        source: io::Error,
    },

    UnableToReadData {
        source: io::Error,
    },

    LengthMismatch {
        expected: u64,
        actual: u64,
    },

    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },

    UnableToDecompressData {
        source: snap::Error,
    },

    UnableToDeserializeData {
        source: prost::DecodeError,
    },

    InvalidMessage {
        source: generated_types::google::FieldViolation,
    },
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ReadArrayError {
    UnableToReadArray { source: io::Error, length: usize },
}

/// The result type returned by the synchronous [`ClosedSegmentReader`].
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SegmentId, FILE_TYPE_IDENTIFIER};
    use assert_matches::assert_matches;
    use byteorder::WriteBytesExt;
    use std::io::Write;
    use test_helpers::assert_error;

    #[test]
    fn successful_read_no_entries() {
        let segment_file = FakeSegmentFile::new();

        let data = segment_file.data();
        let mut reader = ClosedSegmentReader::new(data.as_slice()).expect("must read header");

        assert_eq!(reader.file_type_identifier(), FILE_TYPE_IDENTIFIER);
        assert_eq!(reader.segment_id(), segment_file.id);

        let entry = reader.one_entry().unwrap();
        assert!(entry.is_none());
        assert_eq!(reader.bytes_read(), segment_file.size_bytes());
    }

    #[test]
    fn successful_read_with_entries() {
        let mut segment_file = FakeSegmentFile::new();
        let entry_input_1 = FakeSegmentEntry::new(b"hello");
        segment_file.add_entry(entry_input_1.clone());

        let entry_input_2 = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(entry_input_2.clone());

        let data = segment_file.data();
        let mut reader = ClosedSegmentReader::new(data.as_slice()).expect("must read header");

        assert_eq!(reader.file_type_identifier(), FILE_TYPE_IDENTIFIER);
        assert_eq!(reader.segment_id(), segment_file.id);

        let entry_output_1 = reader.one_entry().unwrap().unwrap();
        let expected_1 = SegmentEntry::from(&entry_input_1);
        assert_eq!(entry_output_1.data, expected_1.data);

        let entry_output_2 = reader.one_entry().unwrap().unwrap();
        let expected_2 = SegmentEntry::from(&entry_input_2);
        assert_eq!(entry_output_2.data, expected_2.data);

        let entry = reader.one_entry().unwrap();
        assert!(entry.is_none());
        assert_eq!(reader.bytes_read(), segment_file.size_bytes());
    }

    #[test]
    fn unsuccessful_read_too_short_len() {
        let mut segment_file = FakeSegmentFile::new();

        // The bad entry will prevent any entries being read, thus the
        // no bytes can be reported as successfully read.
        let want_bytes_read = segment_file.size_bytes();

        let bad_entry_input = FakeSegmentEntry::new(b"hello");
        let good_length = bad_entry_input.compressed_len();
        let bad_entry_input = bad_entry_input.with_compressed_len(good_length - 1);
        segment_file.add_entry(bad_entry_input);

        let good_entry_input = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(good_entry_input);

        let data = segment_file.data();
        let mut reader = ClosedSegmentReader::new(data.as_slice()).expect("must read header");

        assert_eq!(reader.file_type_identifier(), FILE_TYPE_IDENTIFIER);
        assert_eq!(reader.segment_id(), segment_file.id);

        let read_fail = reader.one_entry();
        assert_matches!(read_fail, Err(Error::UnableToReadData { source: e }) => {
            assert_matches!(e.kind(), std::io::ErrorKind::UnexpectedEof);
        });
        assert_eq!(reader.bytes_read(), want_bytes_read);
        // Trying to continue reading will fail as well, see:
        // <https://github.com/influxdata/influxdb_iox/issues/6222>
        assert_error!(reader.one_entry(), Error::UnableToReadData { .. });
        // Ensure no magical bean counting occurs when stuck unable to read data.
        assert_eq!(reader.bytes_read(), want_bytes_read);
    }

    #[test]
    fn unsuccessful_read_too_long_len() {
        let mut segment_file = FakeSegmentFile::new();

        // The bad entry will prevent any entries being read, thus the
        // no bytes can be reported as successfully read.
        let want_bytes_read = segment_file.size_bytes();

        let bad_entry_input = FakeSegmentEntry::new(b"hello");
        let good_length = bad_entry_input.compressed_len();
        let bad_entry_input = bad_entry_input.with_compressed_len(good_length + 1);
        segment_file.add_entry(bad_entry_input);

        let good_entry_input = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(good_entry_input);

        let data = segment_file.data();
        let mut reader = ClosedSegmentReader::new(data.as_slice()).expect("must read header");

        assert_eq!(reader.file_type_identifier(), FILE_TYPE_IDENTIFIER);
        assert_eq!(reader.segment_id(), segment_file.id);

        let read_fail = reader.one_entry();
        assert_matches!(read_fail, Err(Error::UnableToReadData { source: e }) => {
            assert_matches!(e.kind(), std::io::ErrorKind::UnexpectedEof);
        });
        assert_eq!(reader.bytes_read(), want_bytes_read);
        // Trying to continue reading will fail as well, see:
        // <https://github.com/influxdata/influxdb_iox/issues/6222>
        assert_error!(reader.one_entry(), Error::UnableToReadData { .. });
        // Also no magical bean counting when cannot read more.
        assert_eq!(reader.bytes_read(), want_bytes_read);
    }

    #[test]
    fn unsuccessful_read_checksum_mismatch() {
        let mut segment_file = FakeSegmentFile::new();

        let bad_entry_input = FakeSegmentEntry::new(b"hello");
        let good_checksum = bad_entry_input.checksum();
        let bad_entry_input = bad_entry_input.with_checksum(good_checksum + 1);
        segment_file.add_entry(bad_entry_input);

        let good_entry_input = FakeSegmentEntry::new(b"goodbye");
        segment_file.add_entry(good_entry_input.clone());

        let data = segment_file.data();
        let mut reader = ClosedSegmentReader::new(data.as_slice()).expect("must read header");

        assert_eq!(reader.file_type_identifier(), FILE_TYPE_IDENTIFIER);
        assert_eq!(reader.segment_id(), segment_file.id);

        let read_fail = reader.one_entry();
        assert_error!(read_fail, Error::ChecksumMismatch { .. });

        // A bad checksum won't corrupt further entries
        let entry_output_2 = reader.one_entry().unwrap().unwrap();
        let expected_2 = SegmentEntry::from(&good_entry_input);
        assert_eq!(entry_output_2.data, expected_2.data);

        let entry = reader.one_entry().unwrap();
        assert!(entry.is_none());
        assert_eq!(reader.bytes_read(), segment_file.size_bytes());
    }

    #[derive(Debug)]
    struct FakeSegmentFile {
        id: SegmentId,
        entries: Vec<FakeSegmentEntry>,
    }

    impl FakeSegmentFile {
        fn new() -> Self {
            Self {
                id: SegmentId::new(0),
                entries: Default::default(),
            }
        }

        fn add_entry(&mut self, entry: FakeSegmentEntry) {
            self.entries.push(entry);
        }

        fn data(&self) -> Vec<u8> {
            let mut f = Vec::new();

            f.write_all(FILE_TYPE_IDENTIFIER).unwrap();

            let id_bytes = self.id.as_bytes();
            f.write_all(&id_bytes).unwrap();

            for entry in &self.entries {
                f.write_u32::<BigEndian>(entry.checksum()).unwrap();
                f.write_u32::<BigEndian>(entry.compressed_len()).unwrap();
                f.write_all(&entry.compressed_data()).unwrap();
            }

            f
        }

        fn size_bytes(&self) -> u64 {
            std::mem::size_of::<FileTypeIdentifier>() as u64
                + std::mem::size_of::<SegmentIdBytes>() as u64
                + self
                    .entries
                    .iter()
                    .map(|e| {
                        // Each entry is sized by the two 4 byte
                        // header values (checksum and compressed_len)
                        // as well as the length of the compressed data.
                        (std::mem::size_of::<u32>()
                            + std::mem::size_of::<u32>()
                            + e.compressed_data().len()) as u64
                    })
                    .sum::<u64>()
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeSegmentEntry {
        checksum: Option<u32>,
        compressed_len: Option<u32>,
        uncompressed_data: Vec<u8>,
    }

    impl FakeSegmentEntry {
        fn new(data: &[u8]) -> Self {
            Self {
                checksum: None,
                compressed_len: None,
                uncompressed_data: data.to_vec(),
            }
        }

        fn with_compressed_len(self, compressed_len: u32) -> Self {
            Self {
                compressed_len: Some(compressed_len),
                ..self
            }
        }

        fn with_checksum(self, checksum: u32) -> Self {
            Self {
                checksum: Some(checksum),
                ..self
            }
        }

        fn checksum(&self) -> u32 {
            self.checksum.unwrap_or_else(|| {
                let mut hasher = Hasher::new();
                hasher.update(&self.compressed_data());
                hasher.finalize()
            })
        }

        fn compressed_data(&self) -> Vec<u8> {
            let mut encoder = snap::write::FrameEncoder::new(Vec::new());
            encoder.write_all(&self.uncompressed_data).unwrap();
            encoder.into_inner().expect("cannot fail to flush to a Vec")
        }

        fn compressed_len(&self) -> u32 {
            self.compressed_len
                .unwrap_or_else(|| self.compressed_data().len() as u32)
        }
    }

    impl From<&FakeSegmentEntry> for SegmentEntry {
        fn from(fake: &FakeSegmentEntry) -> Self {
            Self {
                data: fake.uncompressed_data.clone(),
            }
        }
    }
}
