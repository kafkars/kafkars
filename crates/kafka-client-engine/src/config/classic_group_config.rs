//! Public raw classic-group timing and its validated core policy bundle.

use std::time::Duration;

use kafka_client_core::{ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicRejoinPolicy};

/// Session, rebalance, Heartbeat, and recovery timing compiled at registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineClassicGroupConfig {
    session_timeout: Duration,
    rebalance_timeout: Duration,
    heartbeat_interval: Duration,
    heartbeat_attempt_timeout: Duration,
    rejoin_backoff: Duration,
    rejoin_attempt_timeout: Duration,
}

impl EngineClassicGroupConfig {
    /// Creates raw classic-group timing for registration validation.
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

    /// Returns the requested Kafka session timeout.
    pub const fn session_timeout(self) -> Duration {
        self.session_timeout
    }

    /// Returns the requested Kafka rebalance timeout.
    pub const fn rebalance_timeout(self) -> Duration {
        self.rebalance_timeout
    }

    /// Returns the requested delay between successful Heartbeats.
    pub const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the requested timeout for one Heartbeat attempt.
    pub const fn heartbeat_attempt_timeout(self) -> Duration {
        self.heartbeat_attempt_timeout
    }

    /// Returns the requested delay before a recoverable rejoin.
    pub const fn rejoin_backoff(self) -> Duration {
        self.rejoin_backoff
    }

    /// Returns the requested timeout for one internally retried Join cycle.
    pub const fn rejoin_attempt_timeout(self) -> Duration {
        self.rejoin_attempt_timeout
    }

    pub(crate) fn validate(self) -> Result<ValidatedClassicGroupConfig, ClassicGroupConfigError> {
        let session_timeout_ms = whole_positive_milliseconds(self.session_timeout)
            .ok_or(ClassicGroupConfigError::SessionTimeout)?;
        let rebalance_timeout_ms = whole_positive_milliseconds(self.rebalance_timeout)
            .ok_or(ClassicGroupConfigError::RebalanceTimeout)?;
        let timing = ClassicGroupTiming::try_new(session_timeout_ms, rebalance_timeout_ms)
            .map_err(|_error| ClassicGroupConfigError::KafkaTiming)?;
        let heartbeat_interval = positive_ticks(self.heartbeat_interval)
            .ok_or(ClassicGroupConfigError::HeartbeatInterval)?;
        let heartbeat_attempt_timeout = positive_ticks(self.heartbeat_attempt_timeout)
            .ok_or(ClassicGroupConfigError::HeartbeatAttemptTimeout)?;
        let heartbeat =
            ClassicHeartbeatPolicy::try_new(heartbeat_interval, heartbeat_attempt_timeout)
                .map_err(|_error| ClassicGroupConfigError::Heartbeat)?;
        let rejoin_backoff =
            positive_ticks(self.rejoin_backoff).ok_or(ClassicGroupConfigError::RejoinBackoff)?;
        let rejoin_attempt_timeout = positive_ticks(self.rejoin_attempt_timeout)
            .ok_or(ClassicGroupConfigError::RejoinAttemptTimeout)?;
        let rejoin = ClassicRejoinPolicy::try_new(rejoin_backoff, rejoin_attempt_timeout)
            .map_err(|_error| ClassicGroupConfigError::Rejoin)?;
        Ok(ValidatedClassicGroupConfig {
            timing,
            heartbeat,
            rejoin,
        })
    }
}

impl Default for EngineClassicGroupConfig {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedClassicGroupConfig {
    timing: ClassicGroupTiming,
    heartbeat: ClassicHeartbeatPolicy,
    rejoin: ClassicRejoinPolicy,
}

impl ValidatedClassicGroupConfig {
    pub(crate) const fn timing(self) -> ClassicGroupTiming {
        self.timing
    }

    pub(crate) const fn heartbeat(self) -> ClassicHeartbeatPolicy {
        self.heartbeat
    }

    pub(crate) const fn rejoin(self) -> ClassicRejoinPolicy {
        self.rejoin
    }
}

impl Default for ValidatedClassicGroupConfig {
    fn default() -> Self {
        EngineClassicGroupConfig::default()
            .validate()
            .unwrap_or_else(|_error| unreachable!("fixed classic-group timing is valid"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassicGroupConfigError {
    SessionTimeout,
    RebalanceTimeout,
    KafkaTiming,
    HeartbeatInterval,
    HeartbeatAttemptTimeout,
    Heartbeat,
    RejoinBackoff,
    RejoinAttemptTimeout,
    Rejoin,
}

fn whole_positive_milliseconds(duration: Duration) -> Option<u64> {
    if duration.is_zero() || duration.subsec_nanos() % 1_000_000 != 0 {
        return None;
    }
    u64::try_from(duration.as_millis()).ok()
}

fn positive_ticks(duration: Duration) -> Option<u64> {
    if duration.is_zero() {
        return None;
    }
    u64::try_from(duration.as_nanos()).ok()
}
