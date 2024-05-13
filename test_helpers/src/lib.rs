// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{
    env, f64,
    sync::{Arc, Once},
};
pub use tempfile;
pub mod prometheus;
pub mod timeout;
pub mod tracing;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// A test helper function for asserting floating point numbers are within the
/// machine epsilon because strict comparison of floating point numbers is
/// incorrect
pub fn approximately_equal(f1: f64, f2: f64) -> bool {
    (f1 - f2).abs() < f64::EPSILON
}

pub fn all_approximately_equal(f1: &[f64], f2: &[f64]) -> bool {
    f1.len() == f2.len() && f1.iter().zip(f2).all(|(&a, &b)| approximately_equal(a, b))
}

/// Return a temporary directory that is deleted when the object is dropped
pub fn tmp_dir() -> Result<tempfile::TempDir> {
    let _ = dotenvy::dotenv();

    let root = env::var_os("TEST_INFLUXDB_IOX_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    Ok(tempfile::Builder::new()
        .prefix("influxdb_iox")
        .tempdir_in(root)?)
}

pub fn tmp_file() -> Result<tempfile::NamedTempFile> {
    let _ = dotenvy::dotenv();

    let root = env::var_os("TEST_INFLUXDB_IOX_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    Ok(tempfile::Builder::new()
        .prefix("influxdb_iox")
        .tempfile_in(root)?)
}

/// Writes the specified string to a new temporary file, returning the Path to
/// the file
pub fn make_temp_file<C: AsRef<[u8]>>(contents: C) -> tempfile::NamedTempFile {
    let file = tmp_file().expect("creating temp file");

    std::fs::write(&file, contents).expect("writing data to temp file");
    file
}

/// convert form that is easier to type in tests to what some code needs
pub fn str_vec_to_arc_vec(str_vec: &[&str]) -> Vec<Arc<str>> {
    str_vec.iter().map(|s| Arc::from(*s)).collect()
}

/// convert form that is easier to type in tests to what some code needs
pub fn str_pair_vec_to_vec(str_vec: &[(&str, &str)]) -> Vec<(Arc<str>, Arc<str>)> {
    str_vec
        .iter()
        .map(|(s1, s2)| (Arc::from(*s1), Arc::from(*s2)))
        .collect()
}

static LOG_SETUP: Once = Once::new();

/// Enables debug logging regardless of the value of RUST_LOG
/// environment variable. If RUST_LOG isn't specifies, defaults to
/// "debug"
///
/// Hint: Try running your test with `--no-capture` if you don't see expected logs.
///
/// This is likely useful only when debugging a single tests or when running
/// with `--test-threads=1` , otherwise outputs will be interleaved because
/// test execution is multi-threaded.
pub fn start_logging() {
    use tracing_log::LogTracer;
    use tracing_subscriber::{filter::EnvFilter, FmtSubscriber};

    // ensure the global has been initialized
    LOG_SETUP.call_once(|| {
        // honor any existing RUST_LOG level
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "debug");
        }

        LogTracer::init().expect("Cannot init log->trace integration");

        let subscriber = FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            // Note `with_test_writer` allows libtest (used for all
            // our tests and invoked by `cargo test`) to capture
            // per-test logging. The captured data will only be
            // shown for failed tests. Pass `--no-capture` to
            // disable that feature (but only try to run a single
            // test to prevent output interleaving).
            .with_test_writer()
            .finish();

        observability_deps::tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    })
}

/// Enables debug logging if the RUST_LOG environment variable is
/// set. Does nothing if RUST_LOG is not set. If enable_logging has
/// been set previously, does nothing
pub fn maybe_start_logging() {
    if std::env::var("RUST_LOG").is_ok() {
        start_logging()
    }
}

#[macro_export]
/// A macro to assert that one string is contained within another with
/// a nice error message if they are not.
///
/// Usage: `assert_contains!(actual, expected)`
///
/// Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertable into `String`s (`Into<String>`)
macro_rules! assert_contains {
    ($ACTUAL: expr, $EXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let expected_value: String = $EXPECTED.into();
        assert!(
            actual_value.contains(&expected_value),
            "Can not find expected in actual.\n\nExpected:\n{}\n\nActual:\n{}",
            expected_value,
            actual_value
        );
    };
}

#[macro_export]
/// A macro to assert that one string is NOT contained within another with
/// a nice error message if that check fails. Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertable into `String`s (`Into<String>`)
macro_rules! assert_not_contains {
    ($ACTUAL: expr, $UNEXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let unexpected_value: String = $UNEXPECTED.into();
        assert!(
            !actual_value.contains(&unexpected_value),
            "Found unexpected value in actual.\n\nUnexpected:\n{}\n\nActual:\n{}",
            unexpected_value,
            actual_value
        );
    };
}

#[macro_export]
/// Assert that an operation fails with one particular error. Panics if the operation succeeds.
/// Prints debug format of the error value if it doesn't match the specified pattern.
macro_rules! assert_error {
    ($OPERATION: expr, $(|)? $( $ERROR_PATTERN:pat_param )|+ $( if $GUARD: expr )? $(,)?) => {
        let err = $OPERATION.unwrap_err();
        assert!(
            matches!(err, $( $ERROR_PATTERN )|+ $( if $GUARD )?),
            "Expected {}, but got {:?}",
            stringify!($( $ERROR_PATTERN )|+ $( if $GUARD )?),
            err
        );
    };
}

#[macro_export]
/// Assert that `actual` and `expected` values are within `epsilon` of each other. Used to compare
/// values that may fluctuate from run to run (e.g. because they encode timestamps)
///
/// Usage: `assert_close!(actual, expected, epsilon);`
macro_rules! assert_close {
    ($ACTUAL:expr, $EXPECTED:expr, $EPSILON:expr) => {{
        {
            let actual = $ACTUAL;
            let expected = $EXPECTED;
            let epsilon = $EPSILON;
            // determine how far apart they actually are
            let delta = actual.abs_diff(expected);
            assert!(
                delta <= epsilon,
                "{} and {} differ by {}, which is more than allowed {}",
                actual,
                expected,
                delta,
                epsilon
            )
        }
    }};
}
