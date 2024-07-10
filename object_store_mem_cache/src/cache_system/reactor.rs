use std::time::{Duration, Instant};

use futures::{stream::BoxStream, FutureExt, StreamExt};
use metric::U64Counter;
use observability_deps::tracing::info;
use tokio::{runtime::Handle, task::JoinSet, time::MissedTickBehavior};

pub(crate) type Trigger = BoxStream<'static, ()>;
pub(crate) type Reaction = Box<dyn Fn() + Send>;

/// Helper for [`TriggerExt::throttle`].
struct ThrottleState {
    trigger: Trigger,
    wait_until: Option<Instant>,
}

/// Extension trait for [triggers](Trigger).
pub(crate) trait TriggerExt {
    /// Throttle given trigger.
    ///
    /// Trigger events that happen during the backoff are dropped.
    fn throttle(self, backoff: Duration) -> Trigger;

    /// Observe trigger events.
    fn observe(
        self,
        cache: &'static str,
        trigger: &'static str,
        metrics: &metric::Registry,
    ) -> Trigger;
}

impl TriggerExt for Trigger {
    fn throttle(self, backoff: Duration) -> Trigger {
        futures::stream::unfold(
            ThrottleState {
                trigger: self,
                wait_until: None,
            },
            move |mut state| async move {
                if let Some(wait_until) = state.wait_until.take() {
                    let mut sleep_fut =
                        std::pin::pin!(tokio::time::sleep_until(wait_until.into()).fuse());

                    loop {
                        tokio::select! {
                            _ = &mut sleep_fut => {
                                break;
                            }
                            maybe = state.trigger.next() => {
                                match maybe {
                                    Some(()) => {
                                        continue;
                                    }
                                    None => {
                                        return None;
                                    }
                                }
                            }
                        }
                    }
                }

                state.trigger.next().await;
                state.wait_until = Some(Instant::now() + backoff);
                Some(((), state))
            },
        )
        .boxed()
    }

    fn observe(
        self,
        cache: &'static str,
        trigger: &'static str,
        metrics: &metric::Registry,
    ) -> Trigger {
        let counter = metrics
            .register_metric::<U64Counter>(
                "mem_cache_reactor_triggered",
                "Actions triggered for the in-mem cache reactor",
            )
            .recorder(&[("cache", cache), ("trigger", trigger)]);
        self.inspect(move |()| {
            info!(cache, trigger, "reactor triggered");
            counter.inc(1);
        })
        .boxed()
    }
}

/// Produce trigger events in regular intervals.
pub(crate) fn ticker(delta: Duration) -> Trigger {
    let mut interval = tokio::time::interval(delta);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.reset(); // otherwise first tick return immediately

    futures::stream::unfold(interval, |mut interval| async move {
        interval.tick().await;
        Some(((), interval))
    })
    .boxed()
}

/// React to [triggers](Trigger) by a [reaction](Reaction).
#[derive(Debug)]
pub(crate) struct Reactor {
    _task: JoinSet<()>,
}

impl Reactor {
    /// Create new reactor given the triggers and a single reaction.
    ///
    /// The handle is used to run this in a background task.
    pub(crate) fn new(
        triggers: impl IntoIterator<Item = Trigger>,
        reaction: Reaction,
        handle: &Handle,
    ) -> Self {
        let mut task = JoinSet::new();
        let mut triggers = futures::stream::select_all(triggers);
        task.spawn_on(
            async move {
                while let Some(()) = triggers.next().await {
                    reaction();
                }
            },
            handle,
        );

        Self { _task: task }
    }
}
