//! Deadline-first assignment, position-control, and close admission.

use std::time::Duration;

use kafka_client_core::{
    AssignedConsumerInput, AssignedTopicPartition, AssignmentEpoch, StartPosition,
};

use super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::AssignedConsumerOwnerError, assigned_topics::AssignedPartitionInput,
};

impl AssignedConsumerOwner {
    /// Replaces all assigned partitions after one deadline-first, two-phase preparation.
    pub(crate) fn replace_assignment(
        &mut self,
        entries: Vec<AssignedPartitionInput>,
        resolution_timeout: Duration,
    ) -> Result<AssignmentEpoch, AssignedConsumerOwnerError> {
        let capture = self
            .clock
            .capture_deadline_after(resolution_timeout)
            .map_err(AssignedConsumerOwnerError::Clock)?;
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
        let transition = self
            .machine
            .apply(AssignedConsumerInput::Assign {
                partitions,
                now: capture.now(),
                resolution_deadline: capture.deadline(),
            })
            .map_err(AssignedConsumerOwnerError::Core)?;
        let epoch = transition.assignment_epoch();
        prepared.commit();
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
        self.ensure_admission_ready()?;
        let transition = self
            .machine
            .apply(AssignedConsumerInput::Resume {
                assignment_epoch,
                partition,
                now: capture.now(),
                resolution_deadline: capture.deadline(),
            })
            .map_err(AssignedConsumerOwnerError::Core)?;
        self.enqueue_transition(transition, Some(capture.operation_deadline()));
        Ok(())
    }

    /// Replaces one partition position with one unchanged boundary deadline.
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
        self.ensure_admission_ready()?;
        let transition = self
            .machine
            .apply(AssignedConsumerInput::Seek {
                assignment_epoch,
                partition,
                position,
                now: capture.now(),
                resolution_deadline: capture.deadline(),
            })
            .map_err(AssignedConsumerOwnerError::Core)?;
        self.enqueue_transition(transition, Some(capture.operation_deadline()));
        Ok(())
    }

    /// Reserves terminal capacity before core can accept close.
    pub(crate) fn begin_close(&mut self) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        self.close
            .reserve()
            .map_err(AssignedConsumerOwnerError::Close)?;
        let transition = match self.machine.apply(AssignedConsumerInput::BeginClose) {
            Ok(transition) => transition,
            Err(error) => {
                if let Err(release) = self.close.release_rejected() {
                    self.fault = Some(AssignedConsumerOwnerFault::Close(release));
                    return Err(AssignedConsumerOwnerError::Faulted);
                }
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        self.enqueue_transition(transition, None);
        Ok(())
    }

    fn ensure_admission_ready(&self) -> Result<(), AssignedConsumerOwnerError> {
        if self.is_faulted() {
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        if !self.effects.is_empty() {
            return Err(AssignedConsumerOwnerError::EffectsPending);
        }
        Ok(())
    }
}
