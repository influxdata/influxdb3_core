// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use chrono::{DateTime, DurationRound, TimeDelta, TimeZone, Timelike, Utc};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, RwLock};
use std::{
    fmt::{Debug, Display},
    future::Future,
    ops::{Add, Sub},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

/// A UTC Timestamp returned by a [`TimeProvider`]
///
/// Purposefully does not provide [`std::convert::From`] implementations
/// as intended to be an opaque type returned by a `TimeProvider` - the construction methods
/// provided are intended for serialization/deserialization and tests only
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Time(DateTime<Utc>);

impl Add<Duration> for Time {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        let duration = chrono::Duration::from_std(rhs).unwrap();
        Self(self.0 + duration)
    }
}

impl Sub<Duration> for Time {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        let duration = chrono::Duration::from_std(rhs).unwrap();
        Self(self.0 - duration)
    }
}

impl Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_rfc3339())
    }
}

impl Time {
    pub const MAX: Self = Self(DateTime::<Utc>::MAX_UTC);
    pub const MIN: Self = Self(DateTime::<Utc>::MIN_UTC);

    /// Makes a new `Time` from the number of non-leap nanoseconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
    pub fn from_timestamp_nanos(nanos: i64) -> Self {
        Self(Utc.timestamp_nanos(nanos))
    }

    /// Makes a new `Time` from the number of non-leap microseconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
    pub fn from_timestamp_micros(micros: i64) -> Option<Self> {
        Utc.timestamp_micros(micros).single().map(Self)
    }

    /// Makes a new `DateTime` from the number of non-leap milliseconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
    pub fn from_timestamp_millis(millis: i64) -> Option<Self> {
        Utc.timestamp_millis_opt(millis).single().map(Self)
    }

    /// Makes a new `Time` from the number of non-leap seconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp")
    /// and the number of nanoseconds since the last whole non-leap second.
    pub fn from_timestamp(secs: i64, nanos: u32) -> Option<Self> {
        Utc.timestamp_opt(secs, nanos).single().map(Self)
    }

    /// Makes a new `Time` from the provided [`DateTime<Utc>`]
    pub const fn from_datetime(time: DateTime<Utc>) -> Self {
        Self(time)
    }

    /// Returns an RFC 3339 and ISO 8601 date and time string such as `1996-12-19T16:39:57+00:00`.
    pub fn to_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }

    /// Parses data from RFC 3339 format.
    pub fn from_rfc3339(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(DateTime::<Utc>::from(
            DateTime::parse_from_rfc3339(s).map_err(Box::new)?,
        )))
    }

    /// Returns the number of non-leap-nanoseconds since January 1, 1970 UTC
    pub fn timestamp_nanos(&self) -> i64 {
        // TODO: ensure that this can never over-/underflow
        self.0.timestamp_nanos_opt().expect("nanos in range")
    }

    /// Returns the number of seconds since January 1, 1970 UTC
    pub fn timestamp(&self) -> i64 {
        self.0.timestamp()
    }

    /// Returns the hour number from 0 to 23.
    pub fn hour(&self) -> u32 {
        self.0.hour()
    }

    /// Returns the minute number from 0 to 59.
    pub fn minute(&self) -> u32 {
        self.0.minute()
    }

    /// Returns the second number from 0 to 59.
    pub fn second(&self) -> u32 {
        self.0.second()
    }

    /// Returns the number of nanoseconds since the last second boundary
    pub fn timestamp_subsec_nanos(&self) -> u32 {
        self.0.timestamp_subsec_nanos()
    }

    /// Returns the number of non-leap-milliseconds since January 1, 1970 UTC
    pub fn timestamp_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }

    /// Returns the duration since the provided time or None if it would be negative
    pub fn checked_duration_since(&self, other: Self) -> Option<Duration> {
        self.0.signed_duration_since(other.0).to_std().ok()
    }

    /// Adds given [`Duration`] to the current date and time.
    ///
    /// Returns `None` if it would result in overflow
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        let duration = chrono::Duration::from_std(duration).ok()?;
        Some(Self(self.0.checked_add_signed(duration)?))
    }

    /// Subtracts the given [`Duration`] from the current date and time.
    ///
    /// Returns `None` if it would result in overflow
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        let duration = chrono::Duration::from_std(duration).ok()?;
        Some(Self(self.0.checked_sub_signed(duration)?))
    }

    /// Returns `Time` as a [`DateTime<Utc>`]
    pub fn date_time(&self) -> DateTime<Utc> {
        self.0
    }

    /// Returns a new instance truncated to the most recently passed hour.
    ///
    /// # Example
    ///
    /// ```
    /// let time = iox_time::Time::from_timestamp_nanos(1723129005000000000);
    /// assert_eq!(time.to_rfc3339(), "2024-08-08T14:56:45+00:00");
    /// let truncated = time.truncate_to_hour().unwrap();
    /// assert_eq!(truncated.to_rfc3339(), "2024-08-08T14:00:00+00:00");
    /// ```
    pub fn truncate_to_hour(&self) -> Result<Self, chrono::RoundingError> {
        self.0.duration_trunc(TimeDelta::hours(1)).map(Self)
    }
}

pub trait TimeProvider: Debug + Display + Send + Sync + 'static {
    /// Returns the current `Time`. No guarantees are made about monotonicity
    fn now(&self) -> Time;

    /// Sleep for the given duration.
    fn sleep(&self, d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        self.sleep_until(self.now() + d)
    }

    /// Sleep until given time.
    fn sleep_until(&self, t: Time) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

    /// Return a time that is the specified number of minutes in the future relative to this
    /// provider's `now`.
    fn minutes_into_future(&self, minutes: u64) -> Time {
        self.now() + Duration::from_secs(60 * minutes)
    }

    /// Return a time that is the specified number of minutes in the past relative to this
    /// provider's `now`.
    fn minutes_ago(&self, minutes_ago: u64) -> Time {
        self.now() - Duration::from_secs(60 * minutes_ago)
    }

    /// Return a time that is the specified number of hours in the past relative to this provider's
    /// `now`.
    fn hours_ago(&self, hours_ago: u64) -> Time {
        self.now() - Duration::from_secs(60 * 60 * hours_ago)
    }
}

/// A [`TimeProvider`] that uses [`Utc::now`] as a clock source
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemProvider {}

impl SystemProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for SystemProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "System")
    }
}

impl TimeProvider for SystemProvider {
    fn now(&self) -> Time {
        Time(Utc::now())
    }

    fn sleep_until(&self, t: Time) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let d = t.checked_duration_since(TimeProvider::now(self));

        Box::pin(async move {
            if let Some(d) = d {
                tokio::time::sleep(d).await;
            }
        })
    }
}

/// Internal state fo [`MockProvider`]
#[derive(Debug)]
struct MockProviderInner {
    now: Time,
    waiting: Vec<Waker>,
}

/// A [`TimeProvider`] that returns a fixed `Time` that can be set by [`MockProvider::set`]
#[derive(Debug, Clone)]
pub struct MockProvider {
    inner: Arc<RwLock<MockProviderInner>>,
}

impl MockProvider {
    pub fn new(start: Time) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MockProviderInner {
                now: start,
                waiting: vec![],
            })),
        }
    }

    pub fn set(&self, time: Time) {
        let mut inner = self.inner.write();
        inner.now = time;
        for waiter in inner.waiting.drain(..) {
            waiter.wake()
        }
    }

    pub fn inc(&self, duration: Duration) -> Time {
        let mut inner = self.inner.write();
        inner.now = inner.now + duration;
        for waiter in inner.waiting.drain(..) {
            waiter.wake()
        }
        inner.now
    }
}

impl Display for MockProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mock")
    }
}

impl TimeProvider for MockProvider {
    fn now(&self) -> Time {
        self.inner.read().now
    }

    fn sleep_until(&self, t: Time) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(MockSleep {
            inner: Arc::clone(&self.inner),
            deadline: t,
        })
    }
}

struct MockSleep {
    inner: Arc<RwLock<MockProviderInner>>,
    deadline: Time,
}

impl Future for MockSleep {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.upgradable_read();
        if inner.now >= self.deadline {
            Poll::Ready(())
        } else {
            let mut inner = RwLockUpgradableReadGuard::upgrade(inner);
            inner.waiting.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> TimeProvider for Arc<T>
where
    T: TimeProvider + ?Sized,
{
    fn now(&self) -> Time {
        (**self).now()
    }

    fn sleep(&self, d: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        (**self).sleep(d)
    }

    fn sleep_until(&self, t: Time) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        (**self).sleep_until(t)
    }
}

/// An asynchronous version of [`TimeProvider`]. This trait provides
/// the same functionality as `TimeProvider` - but the idea is that
/// looking at the clock is async.
pub trait AsyncTimeProvider: Debug + Display + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the current `Time`. No guarantees are made about monotonicity
    fn now(&self) -> impl std::future::Future<Output = Result<Time, Self::Error>> + Send;
}

impl AsyncTimeProvider for SystemProvider {
    type Error = std::convert::Infallible;

    async fn now(&self) -> Result<Time, Self::Error> {
        Ok(TimeProvider::now(self))
    }
}

impl AsyncTimeProvider for MockProvider {
    type Error = std::convert::Infallible;

    async fn now(&self) -> Result<Time, Self::Error> {
        Ok(TimeProvider::now(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_system_provider_now() {
        let provider = SystemProvider::new();
        let (a, async_a) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );
        std::thread::sleep(Duration::from_secs(1));
        let (b, async_b) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );
        let (c, async_c) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );

        let delta = b.checked_duration_since(a).unwrap();
        let async_delta = async_b.checked_duration_since(async_a).unwrap();

        // Assert that the deltas are within a millisecond of each other,
        // they should be relatively close but not necessarily equal.
        assert!(delta.abs_diff(async_delta) <= Duration::from_millis(1));

        assert!(delta > Duration::from_millis(500));
        assert!(delta < Duration::from_secs(5));
        assert!(b <= c);

        assert!(async_delta > Duration::from_millis(500));
        assert!(async_delta < Duration::from_secs(5));
        assert!(async_b <= async_c);
    }

    #[tokio::test]
    async fn test_system_provider_sleep() {
        let provider = SystemProvider::new();

        let (a, async_a) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );
        TimeProvider::sleep(&provider, Duration::from_secs(1)).await;
        let (b, async_b) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );

        let delta = b.checked_duration_since(a).unwrap();
        let async_delta = async_b.checked_duration_since(async_a).unwrap();

        // Assert that the deltas are within a millisecond of each other,
        // they should be relatively close but not necessarily equal.
        assert!(delta.abs_diff(async_delta) <= Duration::from_millis(1));
        assert!(delta > Duration::from_millis(500));
        assert!(delta < Duration::from_secs(5));

        assert!(async_delta > Duration::from_millis(500));
        assert!(async_delta < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_system_provider_sleep_until() {
        let provider = SystemProvider::new();

        let (a, async_a) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );
        TimeProvider::sleep_until(&provider, a + Duration::from_secs(1)).await;
        let (b, async_b) = (
            TimeProvider::now(&provider),
            AsyncTimeProvider::now(&provider).await.unwrap(),
        );

        let delta = b.checked_duration_since(a).unwrap();
        let async_delta = async_b.checked_duration_since(async_a).unwrap();

        // Assert that the deltas are within a millisecond of each other,
        // they should be relatively close but not necessarily equal.
        assert!(delta.abs_diff(async_delta) <= Duration::from_millis(1));
        assert!(delta > Duration::from_millis(500));
        assert!(delta < Duration::from_secs(5));

        assert!(async_delta > Duration::from_millis(500));
        assert!(async_delta < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_mock_provider_now() {
        let provider = MockProvider::new(Time::from_timestamp_nanos(0));
        assert_eq!(TimeProvider::now(&provider).timestamp_nanos(), 0);
        assert_eq!(
            AsyncTimeProvider::now(&provider).await.unwrap(),
            TimeProvider::now(&provider)
        );
        assert_eq!(
            AsyncTimeProvider::now(&provider)
                .await
                .unwrap()
                .timestamp_nanos(),
            0
        );

        provider.set(Time::from_timestamp_nanos(12));
        assert_eq!(TimeProvider::now(&provider).timestamp_nanos(), 12);
        assert_eq!(
            AsyncTimeProvider::now(&provider).await.unwrap(),
            TimeProvider::now(&provider)
        );
        assert_eq!(
            AsyncTimeProvider::now(&provider)
                .await
                .unwrap()
                .timestamp_nanos(),
            12
        );
    }

    #[tokio::test]
    async fn test_mock_provider_sleep() {
        let provider = MockProvider::new(Time::from_timestamp_nanos(0));

        // not sleeping finishes instantly
        provider.sleep(Duration::from_secs(0)).await;

        // ==== sleep with `inc` ====
        let fut = provider.sleep(Duration::from_millis(100));
        let handle = tokio::task::spawn(async move {
            fut.await;
        });

        // does not finish immediately
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // does not finish when not incremented enough
        provider.inc(Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // finishes once incremented at least to the duration
        provider.inc(Duration::from_millis(50));
        handle.await.unwrap();

        // finishes also when "overshooting" the duration
        let fut = provider.sleep(Duration::from_millis(100));
        let handle = tokio::task::spawn(async move {
            fut.await;
        });
        provider.inc(Duration::from_millis(101));
        handle.await.unwrap();

        // ==== sleep with `set` ====
        provider.set(Time::from_timestamp_millis(100).unwrap());
        let fut = provider.sleep(Duration::from_millis(100));
        let handle = tokio::task::spawn(async move {
            fut.await;
        });

        // does not finish immediately
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // does not finish when time goes backwards
        provider.set(Time::from_timestamp_millis(0).unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // does not finish when time goes forward but not enough
        provider.set(Time::from_timestamp_millis(150).unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // finishes when time is set at least to the wait duration
        provider.set(Time::from_timestamp_millis(200).unwrap());
        handle.await.unwrap();

        // also finishes when "overshooting"
        let fut = provider.sleep(Duration::from_millis(100));
        let handle = tokio::task::spawn(async move {
            fut.await;
        });
        provider.set(Time::from_timestamp_millis(301).unwrap());
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_mock_provider_sleep_until() {
        let provider = MockProvider::new(Time::from_timestamp_nanos(0));

        // not sleeping finishes instantly
        provider.sleep(Duration::from_secs(0)).await;

        // ==== sleep with `inc` ====
        let fut = provider.sleep_until(Time::from_timestamp_millis(100).unwrap());
        let handle = tokio::task::spawn(async move {
            fut.await;
        });

        // does not finish immediately
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // does not finish when not incremented enough
        provider.inc(Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // finishes once incremented at least to the duration
        provider.inc(Duration::from_millis(50));
        handle.await.unwrap();

        // finishes also when "overshooting" the duration
        let fut = provider.sleep_until(Time::from_timestamp_millis(200).unwrap());
        let handle = tokio::task::spawn(async move {
            fut.await;
        });
        provider.inc(Duration::from_millis(101));
        handle.await.unwrap();

        // ==== sleep with `set` ====
        provider.set(Time::from_timestamp_millis(100).unwrap());
        let fut = provider.sleep_until(Time::from_timestamp_millis(200).unwrap());
        let handle = tokio::task::spawn(async move {
            fut.await;
        });

        // does not finish immediately
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // does not finish when time goes backwards
        provider.set(Time::from_timestamp_millis(0).unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // does not finish when time goes forward but not enough
        provider.set(Time::from_timestamp_millis(150).unwrap());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle.is_finished());

        // finishes when time is set at least to the wait duration
        provider.set(Time::from_timestamp_millis(200).unwrap());
        handle.await.unwrap();

        // also finishes when "overshooting"
        let fut = provider.sleep_until(Time::from_timestamp_millis(300).unwrap());
        let handle = tokio::task::spawn(async move {
            fut.await;
        });
        provider.set(Time::from_timestamp_millis(301).unwrap());
        handle.await.unwrap();
    }

    #[test]
    fn test_time() {
        let verify = |date_time: DateTime<Utc>| {
            let time = Time::from_datetime(date_time);

            assert_eq!(time.hour(), date_time.hour());
            assert_eq!(time.minute(), date_time.minute());
            assert_eq!(time.second(), date_time.second());

            assert_eq!(time.date_time(), date_time);
            assert_eq!(
                time,
                Time::from_timestamp(date_time.timestamp(), date_time.timestamp_subsec_nanos())
                    .unwrap(),
            );
            assert_eq!(
                time,
                Time::from_timestamp_nanos(date_time.timestamp_nanos_opt().unwrap())
            );
            assert_eq!(
                Time::from_timestamp_millis(date_time.timestamp_millis()).unwrap(),
                Time::from_datetime(
                    Utc.timestamp_millis_opt(date_time.timestamp_millis())
                        .unwrap()
                )
            );

            assert_eq!(
                time.timestamp_nanos(),
                date_time.timestamp_nanos_opt().unwrap()
            );
            assert_eq!(time.timestamp_millis(), date_time.timestamp_millis());
            assert_eq!(time.to_rfc3339(), date_time.to_rfc3339());

            let duration = Duration::from_millis(265367345);

            assert_eq!(
                time + duration,
                Time::from_datetime(date_time + chrono::Duration::from_std(duration).unwrap())
            );

            assert_eq!(
                time - duration,
                Time::from_datetime(date_time - chrono::Duration::from_std(duration).unwrap())
            );

            assert_eq!(time, Time::from_rfc3339(&time.to_rfc3339()).unwrap());
        };

        verify(Utc.timestamp_nanos(3406960448958394583));
        verify(Utc.timestamp_nanos(0));
        verify(Utc.timestamp_nanos(-3659396346346));
    }

    #[test]
    fn test_overflow() {
        let time = Time::MAX;
        assert!(time.checked_add(Duration::from_nanos(1)).is_none());
        assert!(time.checked_sub(Duration::from_nanos(1)).is_some());

        let time = Time::MIN;
        assert!(time.checked_add(Duration::from_nanos(1)).is_some());
        assert!(time.checked_sub(Duration::from_nanos(1)).is_none());

        let duration = Duration::from_millis(i64::MAX as u64 + 1);

        let time = Time::from_timestamp_nanos(0);
        assert!(chrono::Duration::from_std(duration).is_err());
        assert!(time.checked_add(duration).is_none());
        assert!(time.checked_sub(duration).is_none());
    }

    #[test]
    fn test_duration_since() {
        assert_eq!(
            Time::from_timestamp_nanos(5056)
                .checked_duration_since(Time::from_timestamp_nanos(-465))
                .unwrap(),
            Duration::from_nanos(5056 + 465)
        );

        assert!(Time::MAX.checked_duration_since(Time::MIN).is_some());

        assert!(Time::from_timestamp_nanos(505)
            .checked_duration_since(Time::from_timestamp_nanos(506))
            .is_none());
    }

    #[tokio::test]
    async fn test_minutes_ago() {
        let now = "2022-07-07T00:00:00+00:00";
        let ago = "2022-07-06T22:38:00+00:00";

        let provider = MockProvider::new(Time::from_rfc3339(now).unwrap());

        let t = TimeProvider::minutes_ago(&provider, 82);
        assert_eq!(t, Time::from_timestamp_nanos(1657147080000000000));
        assert_eq!(t.to_rfc3339(), ago);
    }

    #[tokio::test]
    async fn test_minutes_into_future() {
        let now = "2022-07-07T00:00:00+00:00";
        let future = "2022-07-07T00:10:00+00:00";

        let provider = MockProvider::new(Time::from_rfc3339(now).unwrap());

        let t = TimeProvider::minutes_into_future(&provider, 10);
        assert_eq!(t, Time::from_timestamp_nanos(1657152600000000000));
        assert_eq!(t.to_rfc3339(), future);
    }

    #[tokio::test]
    async fn test_hours_ago() {
        let now = "2022-07-07T00:00:00+00:00";
        let ago = "2022-07-03T14:00:00+00:00";

        let provider = MockProvider::new(Time::from_rfc3339(now).unwrap());

        let t = TimeProvider::hours_ago(&provider, 82);
        assert_eq!(t, Time::from_timestamp_nanos(1656856800000000000));
        assert_eq!(t.to_rfc3339(), ago);
    }
}
