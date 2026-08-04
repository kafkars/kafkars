//! Raw hosted-group operation durations validated before registration.

use std::time::Duration;

/// Seek and explicit-close durations fixed for one hosted group consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineGroupConsumerOperationConfig {
    seek_timeout: Duration,
    close_timeout: Duration,
}

impl EngineGroupConsumerOperationConfig {
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

    pub(crate) fn validate(
        self,
    ) -> Result<ValidatedGroupConsumerOperationConfig, GroupConsumerOperationConfigError> {
        validate_duration(self.seek_timeout)
            .ok_or(GroupConsumerOperationConfigError::SeekTimeout)?;
        validate_duration(self.close_timeout)
            .ok_or(GroupConsumerOperationConfigError::CloseTimeout)?;
        Ok(ValidatedGroupConsumerOperationConfig {
            seek_timeout: self.seek_timeout,
            close_timeout: self.close_timeout,
        })
    }
}

impl Default for EngineGroupConsumerOperationConfig {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), Duration::from_secs(30))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedGroupConsumerOperationConfig {
    seek_timeout: Duration,
    close_timeout: Duration,
}

impl ValidatedGroupConsumerOperationConfig {
    pub(crate) const fn seek_timeout(self) -> Duration {
        self.seek_timeout
    }

    pub(crate) const fn close_timeout(self) -> Duration {
        self.close_timeout
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupConsumerOperationConfigError {
    SeekTimeout,
    CloseTimeout,
}

fn validate_duration(duration: Duration) -> Option<u64> {
    if duration.is_zero() {
        return None;
    }
    u64::try_from(duration.as_nanos()).ok()
}
