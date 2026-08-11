use crate::db_options::RateLimiterMode;
use crate::ffi;
use libc::c_int;
use std::ptr::NonNull;
use std::sync::Arc;

pub(crate) struct RateLimiterWrapper {
    pub(crate) inner: NonNull<ffi::rocksdb_ratelimiter_t>,
}

unsafe impl Send for RateLimiterWrapper {}
unsafe impl Sync for RateLimiterWrapper {}

impl Drop for RateLimiterWrapper {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_ratelimiter_destroy(self.inner.as_ptr());
        }
    }
}

/// I/O priority for rate-limiter statistics, mirroring rocksdb's
/// `Env::IOPriority`. Compaction output is normally scheduled at
/// [`IoPriority::Low`] and flush output at [`IoPriority::High`], but when the
/// write controller is stalling or stopping writes, both are submitted at
/// [`IoPriority::User`] so they can drain the backlog faster.
/// [`IoPriority::Total`] aggregates across all priorities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum IoPriority {
    Low = 0,
    High = 2,
    User = 3,
    Total = 4,
}

/// A `RateLimiter` can be shared across multiple DB instances to control the
/// total write rate of flush and compaction.
///
/// Use [`Options::set_shared_ratelimiter`](crate::Options::set_shared_ratelimiter) to
/// assign a rate limiter to a DB.
///
/// # Examples
///
/// ```
/// use rust_rocksdb::{Options, RateLimiter, RateLimiterMode};
///
/// let limiter = RateLimiter::new(10 * 1024 * 1024, 100_000, 10, RateLimiterMode::KAllIo, false);
///
/// let mut opts1 = Options::default();
/// opts1.create_if_missing(true);
/// opts1.set_shared_ratelimiter(&limiter);
///
/// let mut opts2 = Options::default();
/// opts2.create_if_missing(true);
/// opts2.set_shared_ratelimiter(&limiter);
/// ```
#[derive(Clone)]
pub struct RateLimiter(pub(crate) Arc<RateLimiterWrapper>);

impl RateLimiter {
    /// Creates a new rate limiter.
    ///
    /// `rate_bytes_per_sec`: controls the total write rate of compaction and
    /// flush in bytes per second. Currently, RocksDB does not enforce rate
    /// limit for anything other than flush and compaction, e.g. write to WAL.
    ///
    /// `refill_period_us`: controls how often tokens are refilled. For example,
    /// when `rate_bytes_per_sec` is set to 10MB/s and `refill_period_us` is set
    /// to 100ms, then 1MB is refilled every 100ms internally. Larger values can
    /// lead to burstier writes while smaller values introduce more CPU overhead.
    ///
    /// `fairness`: RateLimiter accepts high-pri and low-pri requests. A low-pri
    /// request is usually blocked in favor of hi-pri request. This fairness
    /// parameter grants low-pri requests permission by `1/fairness` chance even
    /// when high-pri requests exist to avoid starvation. Default: 10.
    ///
    /// `mode`: indicates which types of operations count against the limit.
    ///
    /// `auto_tuned`: enables dynamic adjustment of rate limit within the range
    /// `[rate_bytes_per_sec / 20, rate_bytes_per_sec]`, according to the recent
    /// demand for background I/O.
    pub fn new(
        rate_bytes_per_sec: i64,
        refill_period_us: i64,
        fairness: i32,
        mode: RateLimiterMode,
        auto_tuned: bool,
    ) -> Self {
        let inner = NonNull::new(unsafe {
            ffi::rocksdb_ratelimiter_create_with_mode(
                rate_bytes_per_sec,
                refill_period_us,
                fairness,
                mode as c_int,
                auto_tuned,
            )
        })
        .unwrap();
        RateLimiter(Arc::new(RateLimiterWrapper { inner }))
    }

    /// The current write rate ceiling in bytes per second. When the limiter was
    /// created with `auto_tuned = true`, this reflects the live auto-tuned value
    /// rather than the configured maximum.
    pub fn bytes_per_second(&self) -> i64 {
        unsafe { ffi::rocksdb_ratelimiter_get_bytes_per_second(self.0.inner.as_ptr()) }
    }

    /// Cumulative bytes admitted (granted) by the limiter for the given
    /// priority. This counts bytes the limiter allowed through, not bytes
    /// physically written to disk.
    pub fn total_bytes_through(&self, priority: IoPriority) -> i64 {
        unsafe {
            ffi::rocksdb_ratelimiter_get_total_bytes_through(
                self.0.inner.as_ptr(),
                priority as c_int,
            )
        }
    }

    /// Cumulative number of requests that passed through the limiter for the
    /// given priority.
    pub fn total_requests(&self, priority: IoPriority) -> i64 {
        unsafe {
            ffi::rocksdb_ratelimiter_get_total_requests(self.0.inner.as_ptr(), priority as c_int)
        }
    }

    /// Number of requests currently waiting for tokens at the given priority.
    ///
    /// Returns `None` when the underlying limiter does not support this query
    /// (rocksdb reports `Status::NotSupported`), so callers can distinguish
    /// "unsupported" from a genuine zero.
    pub fn total_pending_requests(&self, priority: IoPriority) -> Option<i64> {
        let mut pending: i64 = 0;
        let supported = unsafe {
            ffi::rocksdb_ratelimiter_get_total_pending_requests(
                self.0.inner.as_ptr(),
                priority as c_int,
                &mut pending,
            )
        };
        (supported != 0).then_some(pending)
    }
}
