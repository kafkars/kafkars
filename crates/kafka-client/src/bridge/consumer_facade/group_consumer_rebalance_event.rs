//! Exhaustive translation of engine-owned classic-group transitions.

use kafka_client_engine::{
    GroupConsumerEvent as EngineEvent, GroupConsumerRevocationAcknowledgeError,
    GroupConsumerRevocationAcknowledgeErrorKind, GroupConsumerRevocationControl,
    GroupConsumerTryTakeEventError, GroupConsumerTryTakeEventErrorKind,
};

use super::group_consumer_event::translate_assignment;
use crate::consumer::{ConsumerEvent, ConsumerRevocation};
use crate::{ErrorKind, KafkaError};

pub(super) fn translate_group_consumer_event(
    event: EngineEvent,
    revocation: GroupConsumerRevocationControl,
) -> ConsumerEvent {
    match event {
        EngineEvent::PartitionsAssigned(assignment) => {
            ConsumerEvent::PartitionsAssigned(translate_assignment(&assignment))
        }
        EngineEvent::PartitionsRevoked(assignment) => {
            let assignment = translate_assignment(&assignment);
            let completion =
                GroupConsumerRevocationCompletion::new(revocation, assignment.assignment_epoch());
            ConsumerEvent::PartitionsRevoking(ConsumerRevocation::from_parts(
                assignment, completion,
            ))
        }
        EngineEvent::PartitionsLost(assignment) => {
            ConsumerEvent::PartitionsLost(translate_assignment(&assignment))
        }
    }
}

/// Private linear bridge completing one exact public revocation event.
pub(crate) struct GroupConsumerRevocationCompletion {
    control: GroupConsumerRevocationControl,
    assignment_epoch: u64,
    completed: bool,
}

impl GroupConsumerRevocationCompletion {
    const fn new(control: GroupConsumerRevocationControl, assignment_epoch: u64) -> Self {
        Self {
            control,
            assignment_epoch,
            completed: false,
        }
    }

    pub(crate) fn complete(&mut self) -> Result<(), KafkaError> {
        if self.completed {
            return Ok(());
        }
        self.control
            .complete(self.assignment_epoch)
            .map_err(translate_group_consumer_revocation_acknowledgment)?;
        self.completed = true;
        Ok(())
    }
}

impl core::fmt::Debug for GroupConsumerRevocationCompletion {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerRevocationCompletion")
            .field("assignment_epoch", &self.assignment_epoch)
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

pub(super) fn translate_group_consumer_event_observation(
    error: GroupConsumerTryTakeEventError,
) -> KafkaError {
    translate_group_consumer_event_observation_kind(error.kind())
}

fn translate_group_consumer_revocation_acknowledgment(
    error: GroupConsumerRevocationAcknowledgeError,
) -> KafkaError {
    translate_group_consumer_revocation_acknowledgment_kind(error.kind())
}

pub(super) fn translate_group_consumer_revocation_acknowledgment_kind(
    error: GroupConsumerRevocationAcknowledgeErrorKind,
) -> KafkaError {
    let (kind, message) = match error {
        GroupConsumerRevocationAcknowledgeErrorKind::Contended => (
            ErrorKind::Backpressure,
            "group revocation acknowledgment is contended",
        ),
        GroupConsumerRevocationAcknowledgeErrorKind::Closed
        | GroupConsumerRevocationAcknowledgeErrorKind::GroupUnavailable
        | GroupConsumerRevocationAcknowledgeErrorKind::StaleAssignmentEpoch
        | GroupConsumerRevocationAcknowledgeErrorKind::DeadlineElapsed => (
            ErrorKind::State,
            "group revocation acknowledgment is no longer current",
        ),
        GroupConsumerRevocationAcknowledgeErrorKind::Clock
        | GroupConsumerRevocationAcknowledgeErrorKind::HostUnavailable
        | GroupConsumerRevocationAcknowledgeErrorKind::InternalInvariant => (
            ErrorKind::Internal,
            "group revocation acknowledgment ownership is unavailable",
        ),
    };
    KafkaError::new(kind, message)
}

pub(super) fn translate_group_consumer_event_observation_kind(
    kind: GroupConsumerTryTakeEventErrorKind,
) -> KafkaError {
    let (kind, message) = match kind {
        GroupConsumerTryTakeEventErrorKind::Contended => (
            ErrorKind::Backpressure,
            "group event observation is contended",
        ),
        GroupConsumerTryTakeEventErrorKind::HostUnavailable => {
            (ErrorKind::Internal, "group event owner is unavailable")
        }
        GroupConsumerTryTakeEventErrorKind::InternalInvariant => {
            (ErrorKind::Internal, "group event ownership is inconsistent")
        }
    };
    KafkaError::new(kind, message)
}
