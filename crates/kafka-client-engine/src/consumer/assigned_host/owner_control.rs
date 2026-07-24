//! Name-to-identity translation for assigned-consumer position control.

use kafka_client_core::{AssignedTopicPartition, AssignmentEpoch};

use crate::clock::DeadlineCapture;

use super::super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_model::AssignedConsumerOwnerError,
};
use super::{
    AssignedConsumerControlInputError, AssignedConsumerPartition, AssignedConsumerStartPosition,
};

impl AssignedConsumerOwner {
    pub(crate) fn pause_named(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: &AssignedConsumerPartition,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let partition = self.resolve_control_partition(partition)?;
        self.pause(assignment_epoch, partition)
    }

    pub(crate) fn resume_named_captured(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: &AssignedConsumerPartition,
        capture: DeadlineCapture,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let partition = self.resolve_control_partition(partition)?;
        self.resume_captured(assignment_epoch, partition, capture)
    }

    pub(crate) fn seek_named_captured(
        &mut self,
        assignment_epoch: AssignmentEpoch,
        partition: &AssignedConsumerPartition,
        position: AssignedConsumerStartPosition,
        capture: DeadlineCapture,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let partition = self.resolve_control_partition(partition)?;
        let position = position
            .try_into_core()
            .ok_or(AssignedConsumerOwnerError::ControlInput(
                AssignedConsumerControlInputError::NegativeOffset,
            ))?;
        self.seek_captured(assignment_epoch, partition, position, capture)
    }

    fn resolve_control_partition(
        &self,
        partition: &AssignedConsumerPartition,
    ) -> Result<AssignedTopicPartition, AssignedConsumerOwnerError> {
        self.topics
            .control_partition(&partition.topic, partition.partition_index())
            .ok_or(AssignedConsumerOwnerError::ControlInput(
                AssignedConsumerControlInputError::UnknownTopic,
            ))
    }
}
