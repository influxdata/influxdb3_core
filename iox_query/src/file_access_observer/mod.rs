//! Observer that measures how files are accessed during plan execution.
//!
//! This is mostly a temporary measure for <https://github.com/influxdata/influxdb_iox/issues/13063>.

use std::{
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use data_types::ObjectStoreId;
use metric::{Attributes, U64Histogram, U64HistogramOptions};
use parking_lot::Mutex;
use range_set::RangeSet;

mod range_set;

#[derive(Debug)]
pub struct FileAccessObserver {
    metrics: Arc<Metrics>,
    ranges: DashMap<ObjectStoreId, Arc<Mutex<RangeSet>>>,
    coalesce: usize,
}

impl FileAccessObserver {
    pub fn new(metric_registry: &metric::Registry, coalesce: usize) -> Self {
        Self {
            metrics: Arc::new(Metrics::new(metric_registry)),
            ranges: Default::default(),
            coalesce,
        }
    }

    /// Register that a certain file is potentially used by a query execution.
    ///
    /// This shall be called for EVERY singular query and EVERY singular file.
    pub fn open_query_file_scope(&self, id: ObjectStoreId, file_size: usize) -> QueryFileScope {
        let ranges = Arc::clone(
            &self
                .ranges
                .entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(RangeSet::new(&[], self.coalesce)))),
        );

        QueryFileScope {
            coalesce: self.coalesce,
            file_size,
            ranges,
            metrics: Arc::clone(&self.metrics),
            input_bytes: Default::default(),
            input_parts: Default::default(),
            uncached_bytes: Default::default(),
            uncached_parts: Default::default(),
            cached_bytes: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct QueryFileScope {
    coalesce: usize,
    file_size: usize,
    ranges: Arc<Mutex<RangeSet>>,
    metrics: Arc<Metrics>,
    input_bytes: AtomicUsize,
    input_parts: AtomicUsize,
    uncached_bytes: AtomicUsize,
    uncached_parts: AtomicUsize,
    cached_bytes: AtomicUsize,
}

impl QueryFileScope {
    /// Register a single call to [`AsyncFileReader::get_byte_ranges`].
    ///
    ///
    /// [`AsyncFileReader::get_byte_ranges`]: datafusion::parquet::arrow::async_reader::AsyncFileReader::get_byte_ranges
    pub fn request(&self, ranges: &[Range<usize>]) {
        let input = RangeSet::new(ranges, self.coalesce);

        for r in &input {
            self.metrics.part_input_bytes.record(r.len() as u64);
        }

        let input_bytes = input.bytes();
        self.input_bytes.fetch_add(input_bytes, Ordering::SeqCst);
        self.metrics.request_input_bytes.record(input_bytes as u64);

        let input_parts = input.iter().len();
        self.input_parts.fetch_add(input_bytes, Ordering::SeqCst);
        self.metrics.request_input_parts.record(input_parts as u64);

        // lock scope
        let uncached = {
            let mut guard = self.ranges.lock();
            let new_ranges = guard.union(&input);
            let delta = new_ranges.difference(&guard);
            *guard = new_ranges;
            delta
        };

        for r in &uncached {
            self.metrics.part_uncached_bytes.record(r.len() as u64);
        }

        let uncached_bytes_abs = uncached.bytes();
        self.uncached_bytes
            .fetch_add(uncached_bytes_abs, Ordering::SeqCst);
        self.metrics
            .request_uncached_bytes_abs
            .record(uncached_bytes_abs as u64);

        let uncached_bytes_rel = 100 * uncached_bytes_abs / input_bytes;
        self.metrics
            .request_uncached_bytes_rel
            .record(uncached_bytes_rel as u64);

        let uncached_parts = uncached.iter().len();
        self.uncached_parts
            .fetch_add(uncached_parts, Ordering::SeqCst);
        self.metrics
            .request_uncached_parts
            .record(uncached_parts as u64);

        let cached_bytes_abs = input_bytes.saturating_sub(uncached_bytes_abs);
        self.cached_bytes
            .fetch_add(cached_bytes_abs, Ordering::SeqCst);
        self.metrics
            .request_cached_bytes_abs
            .record(cached_bytes_abs as u64);

        let cached_bytes_rel = 100 * cached_bytes_abs / input_bytes;
        self.metrics
            .request_cached_bytes_rel
            .record(cached_bytes_rel as u64);
    }
}

impl Drop for QueryFileScope {
    fn drop(&mut self) {
        let file_size = self.file_size;
        self.metrics.handle_file_bytes.record(file_size as u64);

        let input_bytes = self.input_bytes.load(Ordering::SeqCst);
        self.metrics.handle_input_bytes.record(input_bytes as u64);

        let input_parts = self.input_parts.load(Ordering::SeqCst);
        self.metrics.handle_input_parts.record(input_parts as u64);

        let uncached_bytes_abs = self.uncached_bytes.load(Ordering::SeqCst);
        self.metrics
            .handle_uncached_bytes_abs
            .record(uncached_bytes_abs as u64);

        let uncached_bytes_rel = 100 * uncached_bytes_abs / file_size;
        self.metrics
            .handle_uncached_bytes_rel
            .record(uncached_bytes_rel as u64);

        let uncached_parts = self.uncached_parts.load(Ordering::SeqCst);
        self.metrics
            .handle_uncached_parts
            .record(uncached_parts as u64);

        let cache_bytes = self.cached_bytes.load(Ordering::SeqCst);
        self.metrics.handle_cached_bytes.record(cache_bytes as u64);
    }
}

/// Metrics collected by the observer.
///
/// # Metric Design
///
/// ## Histograms
/// We collect three metric types in histograms (which also by extension include a total sum and a total count).
///
/// - **bytes:** Total number of bytes.
/// - **part counts:** Number many parts (e.g. closed ranges in a single range set). This is likely to imply the number
///   of HTTP/IO requests.
/// - **relative bytes:** Bytes measured in percentage, relative to some baseline (see _scopes_ below), so this is
///   `100 * value / baseline` (in exactly that order to allow for integer arithmetic)
///
/// Each of these histogram types has different bucket configurations.
///
/// ## Scopes / Granularity
/// We collect metrics in the following scopes, i.e. granularity. You can image these as `GROUP BY` clauses:
///
/// - **part:** A single range of data that we request. Multiple of these can be requested in one _request_ (see below).
///   There are NO relative metrics in this scope.
/// - **request:** A single request to the IO layer that can request multiple _parts_ (i.e. closed ranges) in one go.
///   Relative metrics have the baseline _input_, so what was requested by DataFusion before we checked what is cached
///   (also see listing below).
/// - **handle:** A single open file handle, i.e. one file attached to one specific query plan. A single _handle_ can be
///   used to execute multiple _requests_. The baseline for relative metrics is the file size of the underlying file.
///
/// The metric attribute key is `scope`.
///
/// ## Measurement Point
/// We collect metrics at the following points:
///
/// - **input:** The requested data by DataFusion AFTER we de-duplicate ranges/parts and coalesce close ranges/parts.
/// - **uncached:** Data that we would have to fetch from a backing object store because it was NOT cached yet. Ranges/parts
///   in this are also coalesced.
/// - **cached:** The data that was cached.
/// - **file:** Data about the file w/o considering the actual request. This only measures the file size.
///
/// Note that the number of bytes in _cached_ plus _uncached_ might be larger than _input_ due to the coalesce logic.
///
/// The metric attribute key is `where`.
#[derive(Debug)]
struct Metrics {
    part_input_bytes: U64Histogram,
    part_uncached_bytes: U64Histogram,
    request_input_bytes: U64Histogram,
    request_input_parts: U64Histogram,
    request_uncached_bytes_abs: U64Histogram,
    request_uncached_bytes_rel: U64Histogram,
    request_uncached_parts: U64Histogram,
    request_cached_bytes_abs: U64Histogram,
    request_cached_bytes_rel: U64Histogram,
    handle_file_bytes: U64Histogram,
    handle_input_bytes: U64Histogram,
    handle_input_parts: U64Histogram,
    handle_uncached_bytes_abs: U64Histogram,
    handle_uncached_bytes_rel: U64Histogram,
    handle_uncached_parts: U64Histogram,
    handle_cached_bytes: U64Histogram,
}

impl Metrics {
    fn new(registry: &metric::Registry) -> Self {
        let hist_bytes = registry.register_metric_with_options::<U64Histogram, _>(
            "iox_file_access_bytes",
            "File access observer, measuring bytes",
            || {
                U64HistogramOptions::new([
                    0,
                    10,
                    100,
                    1_000,
                    10_000,
                    100_000,
                    1_000_000,
                    10_000_000,
                    100_000_000,
                ])
            },
        );
        let hist_parts = registry.register_metric_with_options::<U64Histogram, _>(
            "iox_file_access_parts",
            "File access observer, measuring individual ranges/parts",
            || U64HistogramOptions::new([0, 10, 100, 1_000]),
        );
        let hist_rel = registry.register_metric_with_options::<U64Histogram, _>(
            "iox_file_access_bytes_precentage",
            "File access observer, measuring a relative size change in percentage, as 100 * new / old",
            || {
                U64HistogramOptions::new([
                    0,
                    10,
                    25,
                    50,
                    75,
                    90,
                    100,
                    110,
                    150,
                    200,
                    500,
                ])
            },
        );

        let attr_range_input = Attributes::from(&[("scope", "part"), ("where", "input")]);
        let attr_range_uncached = Attributes::from(&[("scope", "part"), ("where", "uncached")]);
        let attr_request_input = Attributes::from(&[("scope", "request"), ("where", "input")]);
        let attr_request_uncached =
            Attributes::from(&[("scope", "request"), ("where", "uncached")]);
        let attr_request_cached = Attributes::from(&[("scope", "request"), ("where", "cached")]);
        let attr_handle_file = Attributes::from(&[("scope", "handle"), ("where", "file")]);
        let attr_handle_input = Attributes::from(&[("scope", "handle"), ("where", "input")]);
        let attr_handle_uncached = Attributes::from(&[("scope", "handle"), ("where", "uncached")]);
        let attr_handle_cached = Attributes::from(&[("scope", "handle"), ("where", "cached")]);

        Self {
            part_input_bytes: hist_bytes.recorder(attr_range_input),
            part_uncached_bytes: hist_bytes.recorder(attr_range_uncached),
            request_input_bytes: hist_bytes.recorder(attr_request_input.clone()),
            request_input_parts: hist_parts.recorder(attr_request_input),
            request_uncached_bytes_abs: hist_bytes.recorder(attr_request_uncached.clone()),
            request_uncached_bytes_rel: hist_rel.recorder(attr_request_uncached.clone()),
            request_uncached_parts: hist_parts.recorder(attr_request_uncached),
            request_cached_bytes_abs: hist_bytes.recorder(attr_request_cached.clone()),
            request_cached_bytes_rel: hist_rel.recorder(attr_request_cached),
            handle_file_bytes: hist_bytes.recorder(attr_handle_file),
            handle_input_bytes: hist_bytes.recorder(attr_handle_input.clone()),
            handle_input_parts: hist_parts.recorder(attr_handle_input),
            handle_uncached_bytes_abs: hist_bytes.recorder(attr_handle_uncached.clone()),
            handle_uncached_bytes_rel: hist_rel.recorder(attr_handle_uncached.clone()),
            handle_uncached_parts: hist_parts.recorder(attr_handle_uncached),
            handle_cached_bytes: hist_bytes.recorder(attr_handle_cached),
        }
    }
}
