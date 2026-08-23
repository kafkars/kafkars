//! Retained public observation of the first share-heartbeat terminal.

use kafka_client_core::{GroupId, ShareGroupHeartbeatFailure};

use super::{
    port::ShareConsumerPort, public_registration::ShareConsumerHandle,
    public_state::ShareConsumerStatePortError, registry::ShareConsumerRegistry,
};

/// Stable terminal category retained when the first share heartbeat never succeeds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareConsumerStartupFailureKind {
    /// No usable share coordinator remained before the original deadline.
    CoordinatorUnavailable,
    /// The broker lacks a compatible share-heartbeat version.
    Compatibility,
    /// Driver or host execution ended terminally.
    Execution,
    /// Kafka returned one exact nonzero error code.
    Broker(i16),
    /// A successful response violated protocol bounds or shape.
    InvalidResponse,
    /// The original membership-start deadline elapsed.
    DeadlineElapsed,
}

impl ShareConsumerHandle {
    /// Returns the retained startup terminal, if membership failed before first success.
    #[doc(hidden)]
    pub fn startup_failure(&self) -> Option<ShareConsumerStartupFailureKind> {
        self.port
            .try_share_startup_failure(self.group_id)
            .ok()
            .flatten()
            .map(startup_failure_kind)
    }
}

impl ShareConsumerPort {
    fn try_share_startup_failure(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareGroupHeartbeatFailure>, ShareConsumerStatePortError> {
        let registry = self
            .shared
            .try_registry()
            .map_err(ShareConsumerStatePortError::Lock)?;
        registry.startup_failure(group_id)
    }
}

impl ShareConsumerRegistry {
    pub(super) fn startup_failure(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ShareGroupHeartbeatFailure>, ShareConsumerStatePortError> {
        let entry = self
            .entry(group_id)
            .ok_or(ShareConsumerStatePortError::Unknown)?;
        Ok(entry.fault.or_else(|| {
            entry
                .membership
                .as_ref()
                .and_then(super::ShareMembershipInterpreter::startup_failure)
        }))
    }
}

const fn startup_failure_kind(
    failure: ShareGroupHeartbeatFailure,
) -> ShareConsumerStartupFailureKind {
    match failure {
        ShareGroupHeartbeatFailure::DeadlineElapsed => {
            ShareConsumerStartupFailureKind::DeadlineElapsed
        }
        ShareGroupHeartbeatFailure::CoordinatorUnavailable => {
            ShareConsumerStartupFailureKind::CoordinatorUnavailable
        }
        ShareGroupHeartbeatFailure::Compatibility => ShareConsumerStartupFailureKind::Compatibility,
        ShareGroupHeartbeatFailure::Execution => ShareConsumerStartupFailureKind::Execution,
        ShareGroupHeartbeatFailure::Broker(code) => ShareConsumerStartupFailureKind::Broker(code),
        ShareGroupHeartbeatFailure::InvalidResponse => {
            ShareConsumerStartupFailureKind::InvalidResponse
        }
    }
}
