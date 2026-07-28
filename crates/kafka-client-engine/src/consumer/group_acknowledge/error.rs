//! Stable processing-acknowledgment failure and checkpoint recovery.

use kafka_client_core::ClassicProcessingLeaseError;

use crate::consumer::{
    GroupConsumerCheckpoint,
    group::{GroupConsumerAcknowledgePortError, GroupConsumerShardLockError},
};

/// Stable reason one checkpoint could not renew application-processing liveness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerAcknowledgeErrorKind {
    /// Engine or group admission has closed.
    Closed,
    /// Another owner currently holds the classic-group shard.
    Contended,
    /// The synchronized engine host can no longer expose the group owner.
    HostUnavailable,
    /// The registered group no longer accepts this operation.
    GroupUnavailable,
    /// The checkpoint no longer names the active assignment.
    StaleCheckpoint,
    /// The processing lease expired before acknowledgment.
    DeadlineElapsed,
    /// Monotonic progress time could not be observed.
    Clock,
    /// Internal ownership was inconsistent.
    InternalInvariant,
}

/// Failed processing acknowledgment retaining the exact linear checkpoint.
#[must_use = "acknowledgment rejection retains the exact checkpoint"]
pub struct GroupConsumerAcknowledgeError {
    kind: GroupConsumerAcknowledgeErrorKind,
    checkpoint: GroupConsumerCheckpoint,
}

impl GroupConsumerAcknowledgeError {
    pub(super) fn from_port(
        error: GroupConsumerAcknowledgePortError,
        checkpoint: GroupConsumerCheckpoint,
    ) -> Self {
        use GroupConsumerAcknowledgeErrorKind as Kind;
        let kind = match error {
            GroupConsumerAcknowledgePortError::Closed => Kind::Closed,
            GroupConsumerAcknowledgePortError::Clock(_) => Kind::Clock,
            GroupConsumerAcknowledgePortError::Lock(GroupConsumerShardLockError::Contended) => {
                Kind::Contended
            }
            GroupConsumerAcknowledgePortError::Lock(GroupConsumerShardLockError::Poisoned) => {
                Kind::HostUnavailable
            }
            GroupConsumerAcknowledgePortError::UnknownGroup
            | GroupConsumerAcknowledgePortError::GroupUnavailable => Kind::GroupUnavailable,
            GroupConsumerAcknowledgePortError::StaleCheckpoint
            | GroupConsumerAcknowledgePortError::Processing(
                ClassicProcessingLeaseError::NotActive | ClassicProcessingLeaseError::FenceMismatch,
            ) => Kind::StaleCheckpoint,
            GroupConsumerAcknowledgePortError::Expired(_)
            | GroupConsumerAcknowledgePortError::Processing(
                ClassicProcessingLeaseError::ExpirationPending,
            ) => Kind::DeadlineElapsed,
            GroupConsumerAcknowledgePortError::Processing(
                ClassicProcessingLeaseError::AlreadyActive
                | ClassicProcessingLeaseError::DeadlineNotElapsed
                | ClassicProcessingLeaseError::DeadlineOverflow,
            )
            | GroupConsumerAcknowledgePortError::UnexpectedEffect => Kind::InternalInvariant,
        };
        Self { kind, checkpoint }
    }

    /// Returns the stable processing-acknowledgment failure category.
    pub const fn kind(&self) -> GroupConsumerAcknowledgeErrorKind {
        self.kind
    }

    /// Recovers the exact checkpoint whose progress was not accepted.
    pub fn into_checkpoint(self) -> GroupConsumerCheckpoint {
        self.checkpoint
    }
}

impl std::fmt::Debug for GroupConsumerAcknowledgeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GroupConsumerAcknowledgeError")
            .field("kind", &self.kind)
            .field("checkpoint", &self.checkpoint)
            .finish()
    }
}

impl std::fmt::Display for GroupConsumerAcknowledgeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "group processing acknowledgment failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerAcknowledgeError {}
