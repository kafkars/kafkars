//! Positive wire-representable classic-group session and rebalance timeouts.

use core::fmt;

/// Smallest timeout accepted by the classic-group timing policy.
pub const CLASSIC_GROUP_TIMEOUT_MIN_MS: u64 = 1;

/// Largest positive timeout accepted by the signed `JoinGroup` wire fields.
pub const CLASSIC_GROUP_TIMEOUT_MAX_MS: u64 = 2_147_483_647;

/// Immutable timeout policy carried by every classic membership cycle.
///
/// These values are independently bounded Kafka protocol scalars. Application
/// processing liveness is a separate lease and does not impose an ordering
/// between these two request fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClassicGroupTiming {
    session_timeout_ms: i32,
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
