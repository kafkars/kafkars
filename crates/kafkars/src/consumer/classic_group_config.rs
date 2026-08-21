//! Public classic-membership timing fixed before group registration.

use std::time::Duration;

/// Session, rebalance, Heartbeat, and retry timing for classic membership.
///
/// Defaults use 10-second session and Heartbeat-attempt timeouts, a 30-second
/// rebalance and rejoin-attempt timeout, a three-second Heartbeat interval, and
/// a one-second rejoin backoff. Group registration rejects zero, unrepresentable,
/// or non-whole-millisecond Kafka request timeouts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGroupConfig {
    session_timeout: Duration,
    rebalance_timeout: Duration,
    heartbeat_interval: Duration,
    heartbeat_attempt_timeout: Duration,
    rejoin_backoff: Duration,
    rejoin_attempt_timeout: Duration,
}

impl ClassicGroupConfig {
    /// Creates one explicit classic-membership timing contract.
    pub const fn new(
        session_timeout: Duration,
        rebalance_timeout: Duration,
        heartbeat_interval: Duration,
        heartbeat_attempt_timeout: Duration,
        rejoin_backoff: Duration,
        rejoin_attempt_timeout: Duration,
    ) -> Self {
        Self {
            session_timeout,
            rebalance_timeout,
            heartbeat_interval,
            heartbeat_attempt_timeout,
            rejoin_backoff,
            rejoin_attempt_timeout,
        }
    }

    /// Returns the Kafka session timeout sent with each classic Join.
    pub const fn session_timeout(self) -> Duration {
        self.session_timeout
    }

    /// Returns the Kafka rebalance timeout sent with each classic Join.
    pub const fn rebalance_timeout(self) -> Duration {
        self.rebalance_timeout
    }

    /// Returns the delay between successful classic Heartbeats.
    pub const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the end-to-end timeout for one classic Heartbeat attempt.
    pub const fn heartbeat_attempt_timeout(self) -> Duration {
        self.heartbeat_attempt_timeout
    }

    /// Returns the delay before a recoverable classic membership retry.
    pub const fn rejoin_backoff(self) -> Duration {
        self.rejoin_backoff
    }

    /// Returns the end-to-end timeout for one internally retried Join cycle.
    pub const fn rejoin_attempt_timeout(self) -> Duration {
        self.rejoin_attempt_timeout
    }

    /// Replaces the Kafka session timeout.
    #[must_use]
    pub const fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    /// Replaces the Kafka rebalance timeout.
    #[must_use]
    pub const fn with_rebalance_timeout(mut self, timeout: Duration) -> Self {
        self.rebalance_timeout = timeout;
        self
    }

    /// Replaces the delay between successful classic Heartbeats.
    #[must_use]
    pub const fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Replaces the end-to-end timeout for one classic Heartbeat attempt.
    #[must_use]
    pub const fn with_heartbeat_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_attempt_timeout = timeout;
        self
    }

    /// Replaces the delay before a recoverable classic membership retry.
    #[must_use]
    pub const fn with_rejoin_backoff(mut self, backoff: Duration) -> Self {
        self.rejoin_backoff = backoff;
        self
    }

    /// Replaces the end-to-end timeout for one internally retried Join cycle.
    #[must_use]
    pub const fn with_rejoin_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.rejoin_attempt_timeout = timeout;
        self
    }

    pub(crate) const fn into_parts(
        self,
    ) -> (Duration, Duration, Duration, Duration, Duration, Duration) {
        (
            self.session_timeout,
            self.rebalance_timeout,
            self.heartbeat_interval,
            self.heartbeat_attempt_timeout,
            self.rejoin_backoff,
            self.rejoin_attempt_timeout,
        )
    }
}

impl Default for ClassicGroupConfig {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(3),
            Duration::from_secs(10),
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
    }
}
