use iox_catalog::{constants::MAX_PARQUET_L0_FILES_PER_PARTITION, interface::Catalog};
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;

pub(crate) async fn perform(
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    default_sleep_interval_minutes: u64,
    dry_run: bool,
) -> Result<()> {
    let mut sleep_interval_minutes = default_sleep_interval_minutes;
    loop {
        if !dry_run {
            let flagged = catalog
                .repositories()
                .parquet_files()
                .flag_for_delete_by_retention() //read/write
                .await
                .context(FlaggingSnafu)?;
            info!(flagged_count = %flagged.len(), "iox_catalog::flag_for_delete_by_retention()");

            if flagged.len() == MAX_PARQUET_L0_FILES_PER_PARTITION as usize {
                // Since this enforcement is run every few minutes, there shouldn't ever be a massive spike
                // in files to delete.  They should trickle in. So if we're deleting the max we can per call,
                // we're probably falling behind day by day.
                if sleep_interval_minutes > 1 {
                    sleep_interval_minutes /= 2;
                }
            } else if sleep_interval_minutes < default_sleep_interval_minutes {
                sleep_interval_minutes *= 2;
            }
        } else {
            debug!("dry run enabled for parquet retention flagger");
        };

        select! {
            _ = shutdown.cancelled() => {
                break
            },
            _ = sleep(Duration::from_secs(60 * sleep_interval_minutes)) => (),
        }
    }
    Ok(())
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Failed to flag parquet files for deletion by retention policy"))]
    Flagging {
        source: iox_catalog::interface::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
