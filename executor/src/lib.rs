//! This module contains a dedicated thread pool for running "cpu
//! intensive" workloads such as DataFusion plans

#![warn(missing_docs)]

use metric::Registry;
use snafu::Snafu;
#[cfg(tokio_unstable)]
use tokio_metrics_bridge::setup_tokio_metrics;
use tokio_watchdog::WatchdogConfig;
// Workaround for "unused crate" lint false positives.
#[cfg(not(tokio_unstable))]
use tokio_metrics_bridge as _;
use workspace_hack as _;

use parking_lot::RwLock;
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{oneshot::error::RecvError, Notify},
    task::JoinSet,
};

use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, TryFutureExt,
};

use observability_deps::tracing::warn;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60 * 5);

/// Errors occuring when polling [`DedicatedExecutor::spawn`].
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum JobError {
    #[snafu(display("Worker thread gone, executor was likely shut down"))]
    WorkerGone,

    #[snafu(display("Panic: {msg}"))]
    Panic { msg: String },
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor
#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<RwLock<State>>,

    /// Used for testing.
    ///
    /// This will ignore explicit shutdown requests.
    testing: bool,
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor.
///
/// The state is only used by the "outer" API, not by the newly created runtime. The new runtime waits for
/// [`start_shutdown`](Self::start_shutdown) and signals the completion via
/// [`completed_shutdown`](Self::completed_shutdown) (for which is owns the sender side).
struct State {
    /// Runtime handle.
    ///
    /// This is `None` when the executor is shutting down.
    handle: Option<Handle>,

    /// If notified, the executor tokio runtime will begin to shutdown.
    ///
    /// We could implement this by checking `handle.is_none()` in regular intervals but requires regular wake-ups and
    /// locking of the state. Just using a proper async signal is nicer.
    start_shutdown: Arc<Notify>,

    /// Receiver side indicating that shutdown is complete.
    completed_shutdown: Shared<BoxFuture<'static, Result<(), Arc<RecvError>>>>,

    /// The inner thread that can be used to join during drop.
    thread: Option<std::thread::JoinHandle<()>>,
}

// IMPORTANT: Implement `Drop` for `State`, NOT for `DedicatedExecutor`, because the executor can be cloned and clones
// share their inner state.
impl Drop for State {
    fn drop(&mut self) {
        if self.handle.is_some() {
            warn!("DedicatedExecutor dropped without calling shutdown()");
            self.handle = None;
            self.start_shutdown.notify_one();
        }

        // do NOT poll the shared future if we are panicking due to https://github.com/rust-lang/futures-rs/issues/2575
        if !std::thread::panicking() && self.completed_shutdown.clone().now_or_never().is_none() {
            warn!("DedicatedExecutor dropped without waiting for worker termination",);
        }

        // join thread but don't care about the results
        self.thread.take().expect("not dropped yet").join().ok();
    }
}

impl std::fmt::Debug for DedicatedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid taking the mutex in debug formatting
        write!(f, "DedicatedExecutor")
    }
}

/// [`DedicatedExecutor`] for testing purposes.
static TESTING_EXECUTOR: OnceLock<DedicatedExecutor> = OnceLock::new();

impl DedicatedExecutor {
    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// executor that is separate from the threadpool created via
    /// `[tokio::main]` or similar.
    ///
    /// The worker thread priority is set to low so that such tasks do
    /// not starve other more important tasks (such as answering health checks)
    ///
    /// Follows the example from to stack overflow and spawns a new
    /// thread to install a Tokio runtime "context"
    /// <https://stackoverflow.com/questions/62536566>
    ///
    /// If you try to do this from a async context you see something like
    /// thread 'plan::stringset::tests::test_builder_plan' panicked at 'Cannot
    /// drop a runtime in a context where blocking is not allowed. This
    /// happens when a runtime is dropped from within an asynchronous
    /// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
    pub fn new(
        name: &str,
        runtime_builder: tokio::runtime::Builder,
        metric_registry: Arc<Registry>,
    ) -> Self {
        Self::new_inner(name, runtime_builder, metric_registry, false)
    }

    fn new_inner(
        name: &str,
        runtime_builder: tokio::runtime::Builder,
        metric_registry: Arc<Registry>,
        testing: bool,
    ) -> Self {
        let name = name.to_owned();

        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        let (tx_handle, rx_handle) = std::sync::mpsc::channel();

        let thread = std::thread::Builder::new()
            .name(format!("{name} driver"))
            .spawn(move || {
                let mut runtime_builder = runtime_builder;
                let runtime = runtime_builder.build().expect("Creating tokio runtime");

                WatchdogConfig::new(runtime.handle(), &metric_registry)
                    .with_runtime_name(&name)
                    .with_tick_duration(Duration::from_millis(100))
                    .with_warn_duration(Duration::from_millis(100))
                    .install();

                #[cfg(tokio_unstable)]
                setup_tokio_metrics(runtime.metrics(), &name, metric_registry);
                #[cfg(not(tokio_unstable))]
                let _ = metric_registry;

                runtime.block_on(async move {
                    // Enable the "notified" receiver BEFORE sending the runtime handle back to the constructor thread
                    // (i.e .the one that runs `new`) to avoid the potential (but unlikely) race that the shutdown is
                    // started right after the constructor finishes and the new runtime calls
                    // `notify_shutdown_captured.notified().await`.
                    //
                    // Tokio provides an API for that by calling `enable` on the `notified` future (this requires
                    // pinning though).
                    let shutdown = notify_shutdown_captured.notified();
                    let mut shutdown = std::pin::pin!(shutdown);
                    shutdown.as_mut().enable();

                    if tx_handle.send(Handle::current()).is_err() {
                        return;
                    }
                    shutdown.await;
                });

                runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);

                // send shutdown "done" signal
                tx_shutdown.send(()).ok();
            })
            .expect("executor setup");

        let handle = rx_handle.recv().expect("driver started");

        let state = State {
            handle: Some(handle),
            start_shutdown: notify_shutdown,
            completed_shutdown: rx_shutdown.map_err(Arc::new).boxed().shared(),
            thread: Some(thread),
        };

        Self {
            state: Arc::new(RwLock::new(state)),
            testing,
        }
    }

    /// Create new executor for testing purposes.
    ///
    /// Internal state may be shared with other tests.
    pub fn new_testing() -> Self {
        TESTING_EXECUTOR
            .get_or_init(|| {
                let mut runtime_builder = tokio::runtime::Builder::new_current_thread();
                runtime_builder.enable_all();

                Self::new_inner(
                    "testing",
                    runtime_builder,
                    Arc::new(Registry::default()),
                    true,
                )
            })
            .clone()
    }

    /// Runs the specified Future (and any tasks it spawns) on the
    /// `DedicatedExecutor`.
    ///
    /// Currently all tasks are added to the tokio executor
    /// immediately and compete for the threadpool's resources.
    pub fn spawn<T>(&self, task: T) -> impl Future<Output = Result<T::Output, JobError>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = {
            let state = self.state.read();
            state.handle.clone()
        };

        let Some(handle) = handle else {
            return futures::future::err(JobError::WorkerGone).boxed();
        };

        // use JoinSet implement "cancel on drop"
        let mut join_set = JoinSet::new();
        join_set.spawn_on(task, &handle);
        async move {
            join_set
                .join_next()
                .await
                .expect("just spawned task")
                .map_err(|e| match e.try_into_panic() {
                    Ok(e) => {
                        let s = if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown internal error".to_string()
                        };

                        JobError::Panic { msg: s }
                    }
                    Err(_) => JobError::WorkerGone,
                })
        }
        .boxed()
    }

    /// signals shutdown of this executor and any Clones
    pub fn shutdown(&self) {
        if self.testing {
            return;
        }

        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.write();
        state.handle = None;
        state.start_shutdown.notify_one();
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    ///
    /// # Panic / Drop
    /// [`DedicatedExecutor`] implements shutdown on [`Drop`]. You should just use this behavior and NOT call
    /// [`join`](Self::join) manually during [`Drop`] or panics because this might lead to another panic, see
    /// <https://github.com/rust-lang/futures-rs/issues/2575>.
    pub async fn join(&self) {
        if self.testing {
            return;
        }

        self.shutdown();

        // get handle mutex is held
        let handle = {
            let state = self.state.read();
            state.completed_shutdown.clone()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        handle.await.expect("Thread died?")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        panic::panic_any,
        sync::{Arc, Barrier},
        time::Duration,
    };
    use tokio::sync::Barrier as AsyncBarrier;

    #[tokio::test]
    async fn basic() {
        let barrier = Arc::new(Barrier::new(2));

        let exec = exec();
        let dedicated_task = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // Note the dedicated task will never complete if it runs on
        // the main tokio thread (as this test is not using the
        // 'multithreaded' version of the executor and the call to
        // barrier.wait actually blocks the tokio thread)
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn basic_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = exec();
        // Run task on clone should work fine
        let dedicated_task = exec.clone().spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn drop_empty_exec() {
        exec();
    }

    #[tokio::test]
    async fn drop_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = exec();

        drop(exec.clone());

        let task = exec.spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn just_panic() {
        struct S(DedicatedExecutor);

        impl Drop for S {
            fn drop(&mut self) {
                self.0.join().now_or_never();
            }
        }

        let exec = exec();
        let _s = S(exec);

        // this must not lead to a double-panic and SIGILL
        panic!("foo")
    }

    #[tokio::test]
    async fn multi_task() {
        let barrier = Arc::new(Barrier::new(3));

        // make an executor with two threads
        let exec = exec2();
        let dedicated_task1 = exec.spawn(do_work(11, Arc::clone(&barrier)));
        let dedicated_task2 = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // block main thread until completion of other two tasks
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task1.await.unwrap(), 11);
        assert_eq!(dedicated_task2.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn tokio_spawn() {
        let exec = exec2();

        // spawn a task that spawns to other tasks and ensure they run on the dedicated
        // executor
        let dedicated_task = exec.spawn(async move {
            // spawn separate tasks
            let t1 = tokio::task::spawn(async { 25usize });
            t1.await.unwrap()
        });

        // Validate the inner task ran to completion (aka it did not panic)
        assert_eq!(dedicated_task.await.unwrap(), 25);

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_str() {
        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("At the disco, on the dedicated task scheduler");
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Panic: At the disco, on the dedicated task scheduler",
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_string() {
        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("{} {}", 1, 2);
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(err.to_string(), "Panic: 1 2",);

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_other() {
        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            if true {
                panic_any(1)
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(err.to_string(), "Panic: unknown internal error",);

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_shutdown_while_task_running() {
        let barrier_1 = Arc::new(Barrier::new(2));
        let captured_1 = Arc::clone(&barrier_1);
        let barrier_2 = Arc::new(Barrier::new(2));
        let captured_2 = Arc::clone(&barrier_2);

        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            captured_1.wait();
            do_work(42, captured_2).await
        });
        barrier_1.wait();

        exec.shutdown();
        // block main thread until completion of the outstanding task
        barrier_2.wait();

        // task should complete successfully
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_submit_task_after_shutdown() {
        let exec = exec();

        // Simulate trying to submit tasks once executor has shutdown
        exec.shutdown();
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Worker thread gone, executor was likely shut down"
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_submit_task_after_clone_shutdown() {
        let exec = exec();

        // shutdown the clone (but not the exec)
        exec.clone().join().await;

        // Simulate trying to submit tasks once executor has shutdown
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Worker thread gone, executor was likely shut down"
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_join() {
        let exec = exec();
        // test it doesn't hang
        exec.join().await;
    }

    #[tokio::test]
    async fn executor_join2() {
        let exec = exec();
        // test it doesn't hang
        exec.join().await;
        exec.join().await;
    }

    #[tokio::test]
    #[allow(clippy::redundant_clone)]
    async fn executor_clone_join() {
        let exec = exec();
        // test it doesn't hang
        exec.clone().join().await;
        exec.clone().join().await;
        exec.join().await;
    }

    #[tokio::test]
    async fn drop_receiver() {
        // create empty executor
        let exec = exec();

        // create first blocked task
        let barrier1_pre = Arc::new(AsyncBarrier::new(2));
        let barrier1_pre_captured = Arc::clone(&barrier1_pre);
        let barrier1_post = Arc::new(AsyncBarrier::new(2));
        let barrier1_post_captured = Arc::clone(&barrier1_post);
        let dedicated_task1 = exec.spawn(async move {
            barrier1_pre_captured.wait().await;
            do_work_async(11, barrier1_post_captured).await
        });
        barrier1_pre.wait().await;

        // create second blocked task
        let barrier2_pre = Arc::new(AsyncBarrier::new(2));
        let barrier2_pre_captured = Arc::clone(&barrier2_pre);
        let barrier2_post = Arc::new(AsyncBarrier::new(2));
        let barrier2_post_captured = Arc::clone(&barrier2_post);
        let dedicated_task2 = exec.spawn(async move {
            barrier2_pre_captured.wait().await;
            do_work_async(22, barrier2_post_captured).await
        });
        barrier2_pre.wait().await;

        // cancel task
        drop(dedicated_task1);

        // cancelation might take a short while
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if Arc::strong_count(&barrier1_post) == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .unwrap();

        // unblock other task
        barrier2_post.wait().await;
        assert_eq!(dedicated_task2.await.unwrap(), 22);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if Arc::strong_count(&barrier2_post) == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .unwrap();

        exec.join().await;
    }

    /// Wait for the barrier and then return `result`
    async fn do_work(result: usize, barrier: Arc<Barrier>) -> usize {
        barrier.wait();
        result
    }

    /// Wait for the barrier and then return `result`
    async fn do_work_async(result: usize, barrier: Arc<AsyncBarrier>) -> usize {
        barrier.wait().await;
        result
    }

    fn exec() -> DedicatedExecutor {
        exec_with_threads(1)
    }

    fn exec2() -> DedicatedExecutor {
        exec_with_threads(2)
    }

    fn exec_with_threads(threads: usize) -> DedicatedExecutor {
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        runtime_builder.worker_threads(threads);
        runtime_builder.enable_all();

        DedicatedExecutor::new(
            "Test DedicatedExecutor",
            runtime_builder,
            Arc::new(Registry::default()),
        )
    }
}
