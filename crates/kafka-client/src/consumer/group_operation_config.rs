//! Public hosted-group seek and close durations fixed before registration.

use std::time::Duration;

/// End-to-end operation durations inherited by one hosted group consumer.
///
/// Both defaults are 30 seconds. Registration rejects zero or durations that
/// cannot fit the engine's monotonic nanosecond domain. Each duration becomes
/// an absolute deadline only when its public seek, close, or shutdown-request
/// call boundary is crossed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupConsumerOperationConfig {
    seek_timeout: Duration,
    close_timeout: Duration,
}

impl GroupConsumerOperationConfig {
    /// Creates one explicit hosted-group operation policy.
    pub const fn new(seek_timeout: Duration, close_timeout: Duration) -> Self {
        Self {
            seek_timeout,
            close_timeout,
        }
    }

    /// Returns the end-to-end duration for one group seek.
    pub const fn seek_timeout(self) -> Duration {
        self.seek_timeout
    }

    /// Returns the end-to-end duration for explicit or requested group close.
    pub const fn close_timeout(self) -> Duration {
        self.close_timeout
    }

    /// Replaces the end-to-end duration for one group seek.
    #[must_use]
    pub const fn with_seek_timeout(mut self, timeout: Duration) -> Self {
        self.seek_timeout = timeout;
        self
    }

    /// Replaces the end-to-end duration for explicit or requested group close.
    #[must_use]
    pub const fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    pub(crate) const fn into_parts(self) -> (Duration, Duration) {
        (self.seek_timeout, self.close_timeout)
    }
}

impl Default for GroupConsumerOperationConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), Duration::from_secs(30))
    }
}
