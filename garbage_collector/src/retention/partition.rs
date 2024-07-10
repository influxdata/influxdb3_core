use iox_catalog::constants::MAX_PARTITION_SELECTED_ONCE_FOR_DELETE;
use iox_catalog::interface::Catalog;
use observability_deps::tracing::*;
use snafu::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;

pub(crate) async fn perform(
    shutdown: CancellationToken,
    catalog: Arc<dyn Catalog>,
    default_sleep_interval_minutes: u64,
) -> Result<()> {
    let mut sleep_interval_minutes = default_sleep_interval_minutes;

    loop {
        let deleted = catalog
            .repositories()
            .partitions()
            .delete_by_retention()
            .await?;
        info!(deleted_count = %deleted.len(), "gc::retention::partition");

        if deleted.len() == MAX_PARTITION_SELECTED_ONCE_FOR_DELETE {
            if sleep_interval_minutes > 1 {
                sleep_interval_minutes /= 2;
            }
        } else if sleep_interval_minutes < default_sleep_interval_minutes {
            sleep_interval_minutes *= 2;
        }

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
    #[snafu(display("Failed to delete partitions by retention"), context(false))]
    Delete {
        source: iox_catalog::interface::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;
