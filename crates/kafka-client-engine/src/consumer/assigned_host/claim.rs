//! One-shot transfer of the sole assigned-consumer application capability.

use std::sync::{Arc, Mutex};

use super::{
    handle::AssignedConsumerHandle, shard::AssignedConsumerPort, state::AssignedConsumerShardState,
};

/// Immediate failure to claim the engine's unique assigned consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssignedConsumerClaimError {
    /// Another clone of the same engine already claimed the consumer.
    AlreadyClaimed,
    /// The internal claim lock was poisoned before ownership could transfer.
    Poisoned,
}

impl std::fmt::Display for AssignedConsumerClaimError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyClaimed => formatter.write_str("assigned consumer was already claimed"),
            Self::Poisoned => formatter.write_str("assigned-consumer claim lock was poisoned"),
        }
    }
}

impl std::error::Error for AssignedConsumerClaimError {}

/// Clone-shared slot whose port can cross the application boundary once.
pub(crate) struct AssignedConsumerClaimSlot {
    port: Mutex<Option<AssignedConsumerPort>>,
}

impl AssignedConsumerClaimSlot {
    pub(crate) fn create_for_engine(
        port: AssignedConsumerPort,
    ) -> (Self, AssignedConsumerAdmissionCloser) {
        let closer = AssignedConsumerAdmissionCloser {
            shared: Arc::clone(&port.shared),
        };
        (
            Self {
                port: Mutex::new(Some(port)),
            },
            closer,
        )
    }

    pub(crate) fn claim(
        &self,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Result<AssignedConsumerHandle, AssignedConsumerClaimError> {
        let mut port = self
            .port
            .lock()
            .map_err(|_poisoned| AssignedConsumerClaimError::Poisoned)?;
        let port = port
            .take()
            .ok_or(AssignedConsumerClaimError::AlreadyClaimed)?;
        Ok(AssignedConsumerHandle::new(port, lifetime))
    }
}

/// Shutdown-only admission fence retained after the application port transfers.
pub(crate) struct AssignedConsumerAdmissionCloser {
    shared: Arc<AssignedConsumerShardState>,
}

impl AssignedConsumerAdmissionCloser {
    pub(crate) fn close(&self) -> Result<(), super::AssignedConsumerShardLockError> {
        self.shared.close_assigned_admission()
    }
}
