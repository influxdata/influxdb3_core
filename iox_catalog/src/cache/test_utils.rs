use std::{future::Future, time::Duration};

pub(crate) trait AssertPendingExt {
    async fn assert_pending(&mut self);
}

impl<F> AssertPendingExt for F
where
    F: Future + Unpin + Send,
{
    async fn assert_pending(&mut self) {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => (),
            _ = self => {
                panic!("not pending");
            }
        }
    }
}
