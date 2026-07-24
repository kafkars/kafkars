//! Deadline-first assignment, position-control, and close admission.

#[cfg(test)]
use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerInput, AssignedTopicPartition, AssignmentEpoch, StartPosition,
};

use crate::clock::DeadlineCapture;

use super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::AssignedConsumerOwnerError, assigned_topics::AssignedPartitionInput,
};

impl AssignedConsumerOwner {
    /// Replaces all assigned partitions after one deadline-first, two-phase preparation.
    #[cfg(test)]
    pub(crate) fn replace_assignment(
        &mut self,
        entries: Vec<AssignedPartitionInput>,
        resolution_timeout: Duration,
    ) -> Result<AssignmentEpoch, AssignedConsumerOwnerError> {
        let capture = self
            .clock
            .capture_deadline_after(resolution_timeout)
            .map_err(AssignedConsumerOwnerError::Clock)?;
        self.replace_assignment_captured(entries, capture)
    }

    /// Applies an assignment with the exact outer-boundary capture unchanged.
    pub(crate) fn replace_assignment_captured(
        &mut self,
        entries: Vec<AssignedPartitionInput>,
        capture: DeadlineCapture,
    ) -> Result<AssignmentEpoch, AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let prepared = self
            .topics
            .prepare_replacement(entries)
            .map_err(AssignedConsumerOwnerError::Topics)?;
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(prepared.partitions().len())
            .map_err(|_error| AssignedConsumerOwnerError::Allocation)?;
        partitions.extend_from_slice(prepared.partitions());
        let partition_count = partitions.len();
        let event_claims = self
            .events
            .prepare_replacement(partition_count)
            .map_err(AssignedConsumerOwnerError::Event)?;
        let transition = match self.machine.apply(AssignedConsumerInput::Assign {
            partitions,
            now: capture.now(),
            resolution_deadline: capture.deadline(),
        }) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        let epoch = transition.assignment_epoch();
        prepared.commit();
        if let Err(error) = event_claims.commit_event_claims(transition.effects()) {
            self.fault = Some(AssignedConsumerOwnerFault::EventTransition { transition, error });
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        let Some(epoch) = epoch else {
            self.retain_transition(transition, Some(capture.operation_deadline()));
            return Err(AssignedConsumerOwnerError::Faulted);
        };
        self.enqueue_transition(transition, Some(capture.operation_deadline()));
        Ok(epoch)
    }

    /// Pauses one exact assignment-fenced partition.
    pub(crate) fn pause(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let transition = self
            .machine
            .apply(AssignedConsumerInput::Pause {
                assignment_epoch,
                partition,
            })
            .map_err(AssignedConsumerOwnerError::Core)?;
        self.enqueue_transition(transition, None);
        Ok(())
    }

    /// Resumes one partition with a deadline captured before any owner work.
    #[cfg(test)]
    pub(crate) fn resume(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        resolution_timeout: Duration,
    ) -> Result<(), AssignedConsumerOwnerError> {
        let capture = self
            .clock
            .capture_deadline_after(resolution_timeout)
            .map_err(AssignedConsumerOwnerError::Clock)?;
        self.resume_captured(assignment_epoch, partition, capture)
    }

    /// Resumes with the exact outer-boundary capture unchanged.
    pub(crate) fn resume_captured(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        capture: DeadlineCapture,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let event_claims = self
            .events
            .prepare_partition(partition)
            .map_err(AssignedConsumerOwnerError::Event)?;
        let transition = match self.machine.apply(AssignedConsumerInput::Resume {
            assignment_epoch,
            partition,
            now: capture.now(),
            resolution_deadline: capture.deadline(),
        }) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        if let Err(error) = event_claims.commit_event_claims(transition.effects()) {
            self.fault = Some(AssignedConsumerOwnerFault::EventTransition { transition, error });
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        self.enqueue_transition(transition, Some(capture.operation_deadline()));
        Ok(())
    }

    /// Replaces one partition position with one unchanged boundary deadline.
    #[cfg(test)]
    pub(crate) fn seek(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        position: StartPosition,
        resolution_timeout: Duration,
    ) -> Result<(), AssignedConsumerOwnerError> {
        let capture = self
            .clock
            .capture_deadline_after(resolution_timeout)
            .map_err(AssignedConsumerOwnerError::Clock)?;
        self.seek_captured(assignment_epoch, partition, position, capture)
    }

    /// Seeks with the exact outer-boundary capture unchanged.
    pub(crate) fn seek_captured(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: AssignedTopicPartition,
        position: StartPosition,
        capture: DeadlineCapture,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let event_claims = self
            .events
            .prepare_partition(partition)
            .map_err(AssignedConsumerOwnerError::Event)?;
        let transition = match self.machine.apply(AssignedConsumerInput::Seek {
            assignment_epoch,
            partition,
            position,
            now: capture.now(),
            resolution_deadline: capture.deadline(),
        }) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        if let Err(error) = event_claims.commit_event_claims(transition.effects()) {
            self.fault = Some(AssignedConsumerOwnerFault::EventTransition { transition, error });
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        self.enqueue_transition(transition, Some(capture.operation_deadline()));
        Ok(())
    }

    pub(super) fn ensure_admission_ready(&self) -> Result<(), AssignedConsumerOwnerError> {
        if self.is_faulted() {
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        if !self.effects.is_empty() {
            return Err(AssignedConsumerOwnerError::EffectsPending);
        }
        Ok(())
    }
}
