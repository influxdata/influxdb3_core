use std::borrow::Cow;
use std::path::PathBuf;
use std::time::Duration;

use metric::{Attributes, U64Gauge};
use sysinfo::Disks;
use tokio::sync::watch;

/// The interval at which disk metrics are updated.
///
/// This is purposely chosen to be out-of-phase w.r.t the default metric scrape
/// interval.
const UPDATE_INTERVAL: Duration = Duration::from_secs(13);

/// An immutable snapshot of space and usage statistics for some disk.
#[derive(Clone, Copy, Debug)]
pub struct DiskSpaceSnapshot {
    available_disk_space: u64,
    total_disk_space: u64,
}

impl DiskSpaceSnapshot {
    /// Create a new disk space snapshot.
    pub fn new(available_disk_space: u64, total_disk_space: u64) -> Self {
        Self {
            available_disk_space,
            total_disk_space,
        }
    }

    /// The available space in bytes on the disk.
    pub fn available_disk_space(&self) -> u64 {
        self.available_disk_space
    }

    /// The maximum capacity in bytes of the disk.
    pub fn total_disk_space(&self) -> u64 {
        self.total_disk_space
    }

    /// Overall usage of the disk, as a percentage [0.0, 1.0].
    #[inline]
    pub fn disk_usage_ratio(&self) -> f64 {
        debug_assert!(self.available_disk_space <= self.total_disk_space);
        1.0 - (self.available_disk_space as f64 / self.total_disk_space as f64)
    }
}

/// A disk space reader that handles the low-level disk space reading logic.
#[derive(Debug)]
pub struct DiskSpaceReader {
    /// The [`Disks`] containing the disk list at construction time.
    disks: Disks,

    /// The index into [`Disks::list()`] for the disk containing the observed
    /// directory.
    disk_idx: usize,
}

impl DiskSpaceReader {
    /// Create a new [`DiskSpaceReader`], returning [`None`] if no disk can be
    /// found for the specified `directory`.
    pub fn new(directory: PathBuf) -> Option<Self> {
        let mut directory = directory.canonicalize().ok()?;

        // Load the disk stats once, and refresh them later.
        let mut disks = Disks::new();
        disks.refresh(true);

        // Resolve the mount point once.
        // The directory path may be `/path/to/dir` and the mount point is `/`.
        let disk_idx = loop {
            if let Some((idx, _disk)) = disks
                .list()
                .iter()
                .enumerate()
                .find(|(_idx, disk)| disk.mount_point() == directory)
            {
                break idx;
            }
            // The mount point for this directory could not be found.
            if !directory.pop() {
                return None;
            }
        };

        Some(Self { disks, disk_idx })
    }

    /// Read the current disk space statistics.
    pub fn read(&mut self) -> DiskSpaceSnapshot {
        let disk = self
            .disks
            .list_mut()
            .get_mut(self.disk_idx)
            .expect("disk list never refreshed so should not change");

        // Refresh the stats for this disk only.
        disk.refresh();

        DiskSpaceSnapshot {
            available_disk_space: disk.available_space(),
            total_disk_space: disk.total_space(),
        }
    }
}

/// A periodic reporter of disk capacity / free statistics for a given
/// directory.
#[derive(Debug)]
pub struct DiskSpaceMetrics {
    available_disk_space: U64Gauge,
    total_disk_space: U64Gauge,

    /// The disk space reader for getting current statistics.
    reader: DiskSpaceReader,

    /// A stream of [`DiskSpaceSnapshot`] produced by the metric reporter for
    /// consumption by any listeners.
    snapshot_tx: watch::Sender<DiskSpaceSnapshot>,
}

impl DiskSpaceMetrics {
    /// Create a new [`DiskSpaceMetrics`], returning [`None`] if no disk can be
    /// found for the specified `directory`.
    pub fn new(
        directory: PathBuf,
        registry: &metric::Registry,
    ) -> Option<(Self, watch::Receiver<DiskSpaceSnapshot>)> {
        let path: Cow<'static, str> = Cow::from(directory.display().to_string());
        let attributes = Attributes::from([("path", path)]);

        let available_disk_space = registry
            .register_metric::<U64Gauge>(
                "disk_space_free",
                "The amount of disk space currently available at the labelled mount point.",
            )
            .recorder(attributes.clone());

        let total_disk_space = registry
            .register_metric::<U64Gauge>(
                "disk_capacity_total",
                "The disk capacity at the labelled mount point.",
            )
            .recorder(attributes);

        let mut reader = DiskSpaceReader::new(directory)?;
        let initial_snapshot = reader.read();

        let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);

        Some((
            Self {
                available_disk_space,
                total_disk_space,
                reader,
                snapshot_tx,
            },
            snapshot_rx,
        ))
    }

    /// Start the [`DiskSpaceMetrics`] evaluation loop, blocking forever.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(UPDATE_INTERVAL);
        loop {
            interval.tick().await;

            let snapshot = self.reader.read();

            self.available_disk_space.set(snapshot.available_disk_space);
            self.total_disk_space.set(snapshot.total_disk_space);

            // Produce and send a [`DiskSpaceSnapshot`] for any listeners
            // that might exist.
            _ = self.snapshot_tx.send(snapshot);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use metric::Metric;
    use tempfile::tempdir_in;
    use test_helpers::timeout::FutureTimeout;

    use super::*;

    #[tokio::test]
    async fn test_metrics() {
        let tmp_dir = tempdir_in(".").ok().unwrap();
        let path = tmp_dir.path().display().to_string();
        // TempDir creates a directory in current directory, so test the relative path (if possible).
        let path = match path.find("/./") {
            Some(index) => &path[index + 3..],
            None => &path[..],
        };

        let pathbuf = PathBuf::from(path);
        let metric_label: Cow<'static, str> = path.to_string().into();

        let registry = Arc::new(metric::Registry::new());

        let (_handle, mut snapshot_rx) =
            DiskSpaceMetrics::new(pathbuf, &registry).expect("root always exists");
        let _handle = tokio::spawn(_handle.run());

        // Wait for the metric to be emitted and non-zero - this should be very
        // quick!
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            // Bound the test duration
            if Instant::now() > deadline {
                panic!("timeout waiting for disk metrics to be resolved");
            }

            let recorded_free_metric = registry
                .get_instrument::<Metric<U64Gauge>>("disk_space_free")
                .expect("metric should exist")
                .get_observer(&Attributes::from([("path", metric_label.clone())]))
                .expect("metric should have labels")
                .fetch();

            let recorded_total_metric = registry
                .get_instrument::<Metric<U64Gauge>>("disk_capacity_total")
                .expect("metric should exist")
                .get_observer(&Attributes::from([("path", metric_label.clone())]))
                .expect("metric should have labels")
                .fetch();

            if recorded_free_metric > 0 && recorded_total_metric > 0 {
                snapshot_rx
                    .changed()
                    .with_timeout_panic(Duration::from_secs(5))
                    .await
                    .expect("snapshot value should have changed");

                let snapshot = *snapshot_rx.borrow();
                assert_eq!(snapshot.available_disk_space, recorded_free_metric);
                assert_eq!(snapshot.total_disk_space, recorded_total_metric);

                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Token test to assert disk usage ratio
    #[test]
    fn assert_disk_usage_ratio() {
        // 80% used
        let snapshot = DiskSpaceSnapshot {
            available_disk_space: 2000,
            total_disk_space: 10000,
        };
        assert_eq!(snapshot.disk_usage_ratio(), 0.8);

        // 90% used
        let snapshot = DiskSpaceSnapshot {
            available_disk_space: 2000,
            total_disk_space: 20000,
        };
        assert_eq!(snapshot.disk_usage_ratio(), 0.9);

        // Free!
        let snapshot = DiskSpaceSnapshot {
            available_disk_space: 42,
            total_disk_space: 42,
        };
        assert_eq!(snapshot.disk_usage_ratio(), 0.0);
    }
}
