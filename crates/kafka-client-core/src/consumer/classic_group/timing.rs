//! Positive wire-representable classic-group session and rebalance timeouts.

use core::fmt;

/// Smallest timeout accepted by the classic-group timing policy.
pub const CLASSIC_GROUP_TIMEOUT_MIN_MS: u64 = 1;

/// Largest positive timeout accepted by the signed `JoinGroup` wire fields.
pub const CLASSIC_GROUP_TIMEOUT_MAX_MS: u64 = 2_147_483_647;

// Engine monotonic observations use nanoseconds, so protocol milliseconds are
// converted here once rather than supplied as an independent liveness setting.
const CORE_TICKS_PER_MILLISECOND: u64 = 1_000_000;

/// Positive deterministic cadence and per-attempt timeout policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicHeartbeatPolicy {
    interval_ticks: u64,
    attempt_timeout_ticks: u64,
}

impl ClassicHeartbeatPolicy {
    /// Validates positive heartbeat cadence and attempt-timeout ticks.
    pub const fn try_new(
        interval_ticks: u64,
        attempt_timeout_ticks: u64,
    ) -> Result<Self, ClassicHeartbeatPolicyError> {
        if interval_ticks == 0 {
            return Err(ClassicHeartbeatPolicyError::IntervalZero);
        }
        if attempt_timeout_ticks == 0 {
            return Err(ClassicHeartbeatPolicyError::AttemptTimeoutZero);
        }
        Ok(Self {
            interval_ticks,
            attempt_timeout_ticks,
        })
    }

    /// Returns the positive delay between successful heartbeat observations.
    pub const fn interval_ticks(self) -> u64 {
        self.interval_ticks
    }

    /// Returns the positive timeout applied to each prepared heartbeat.
    pub const fn attempt_timeout_ticks(self) -> u64 {
        self.attempt_timeout_ticks
    }
}

/// Invalid deterministic heartbeat timing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicHeartbeatPolicyError {
    /// Heartbeat cadence must advance monotonic time.
    IntervalZero,
    /// Every prepared heartbeat must own a future attempt deadline.
    AttemptTimeoutZero,
}

impl fmt::Display for ClassicHeartbeatPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "classic heartbeat policy is invalid: {self:?}")
    }
}

impl std::error::Error for ClassicHeartbeatPolicyError {}

/// Immutable timeout policy carried by every classic membership cycle.
///
/// These values are independently bounded Kafka protocol scalars. Application
/// processing liveness is a separate lease and does not impose an ordering
/// between these two request fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGroupTiming {
    session_timeout_ms: i32,
    session_timeout_ticks: u64,
    rebalance_timeout_ms: i32,
}

impl ClassicGroupTiming {
    /// Validates both millisecond values against Kafka's positive signed domain.
    pub fn try_new(
        session_timeout_ms: u64,
        rebalance_timeout_ms: u64,
    ) -> Result<Self, ClassicGroupTimingError> {
        if session_timeout_ms == 0 || session_timeout_ms > CLASSIC_GROUP_TIMEOUT_MAX_MS {
            return Err(ClassicGroupTimingError::SessionTimeout {
                actual_ms: session_timeout_ms,
            });
        }
        if rebalance_timeout_ms == 0 || rebalance_timeout_ms > CLASSIC_GROUP_TIMEOUT_MAX_MS {
            return Err(ClassicGroupTimingError::RebalanceTimeout {
                actual_ms: rebalance_timeout_ms,
            });
        }
        let session_timeout_ticks = session_timeout_ms * CORE_TICKS_PER_MILLISECOND;
        let session_timeout_ms = i32::try_from(session_timeout_ms).map_err(|_error| {
            ClassicGroupTimingError::SessionTimeout {
                actual_ms: session_timeout_ms,
            }
        })?;
        let rebalance_timeout_ms = i32::try_from(rebalance_timeout_ms).map_err(|_error| {
            ClassicGroupTimingError::RebalanceTimeout {
                actual_ms: rebalance_timeout_ms,
            }
        })?;
        Ok(Self {
            session_timeout_ms,
            session_timeout_ticks,
            rebalance_timeout_ms,
        })
    }

    /// Returns the exact positive signed session-timeout request field.
    pub const fn session_timeout_ms(self) -> i32 {
        self.session_timeout_ms
    }

    /// Returns the exact positive signed rebalance-timeout request field.
    pub const fn rebalance_timeout_ms(self) -> i32 {
        self.rebalance_timeout_ms
    }

    /// Converts the exact retained wire timeout into monotonic nanosecond ticks.
    ///
    /// The constructor caps milliseconds at `i32::MAX`; multiplying that bound
    /// by one million is therefore provably within the `u64` tick domain.
    pub(super) const fn session_timeout_ticks(self) -> u64 {
        self.session_timeout_ticks
    }
}

/// Exact field whose requested value is outside the supported positive domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicGroupTimingError {
    /// Session timeout is zero or exceeds Kafka's positive signed domain.
    SessionTimeout {
        /// Exact rejected value.
        actual_ms: u64,
    },
    /// Rebalance timeout is zero or exceeds Kafka's positive signed domain.
    RebalanceTimeout {
        /// Exact rejected value.
        actual_ms: u64,
    },
}

impl fmt::Display for ClassicGroupTimingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SessionTimeout { actual_ms } => write!(
                formatter,
                "classic group session timeout {actual_ms} ms is outside 1..={CLASSIC_GROUP_TIMEOUT_MAX_MS}"
            ),
            Self::RebalanceTimeout { actual_ms } => write!(
                formatter,
                "classic group rebalance timeout {actual_ms} ms is outside 1..={CLASSIC_GROUP_TIMEOUT_MAX_MS}"
            ),
        }
    }
}

impl std::error::Error for ClassicGroupTimingError {}
