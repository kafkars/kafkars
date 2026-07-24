//! One-partition facade translation over core-owned direct-consumer controls.

use kafka_client_engine::{
    AssignedConsumerHandle as EngineHandle, AssignedConsumerPartition as EnginePartition,
    AssignedConsumerResumeCapture as EngineResumeCapture,
    AssignedConsumerSeekCapture as EngineSeekCapture,
};

use crate::{
    KafkaError,
    consumer::{StartPosition, TopicPartition},
};

use super::{
    consumer_assignment::{AssignedConsumerAssignmentState, engine_start},
    consumer_assignment_result::translate_assigned_assignment_fault,
    consumer_control_result::{
        translate_assigned_control_admission, translate_assigned_control_input,
    },
};

pub(super) fn try_pause(
    handle: &mut EngineHandle,
    assignment: &mut AssignedConsumerAssignmentState,
    partition: &TopicPartition,
) -> Result<(), KafkaError> {
    let partition = engine_partition(partition)?;
    let accepted = handle
        .try_pause(assignment.epoch(), partition)
        .map_err(translate_assigned_control_admission)?;
    retain_diagnostic(assignment, accepted.fault());
    Ok(())
}

pub(super) fn try_resume_captured(
    capture: EngineResumeCapture<'_>,
    assignment: &mut AssignedConsumerAssignmentState,
    partition: &TopicPartition,
) -> Result<(), KafkaError> {
    let partition = engine_partition(partition)?;
    let accepted = capture
        .try_resume(assignment.epoch(), partition)
        .map_err(translate_assigned_control_admission)?;
    retain_diagnostic(assignment, accepted.fault());
    Ok(())
}

pub(super) fn try_seek_captured(
    capture: EngineSeekCapture<'_>,
    assignment: &mut AssignedConsumerAssignmentState,
    partition: &TopicPartition,
    position: StartPosition,
) -> Result<(), KafkaError> {
    let partition = engine_partition(partition)?;
    let accepted = capture
        .try_seek(assignment.epoch(), partition, engine_start(position))
        .map_err(translate_assigned_control_admission)?;
    retain_diagnostic(assignment, accepted.fault());
    Ok(())
}

pub(super) fn engine_partition(partition: &TopicPartition) -> Result<EnginePartition, KafkaError> {
    EnginePartition::try_new(partition.topic(), partition.partition())
        .map_err(translate_assigned_control_input)
}

fn retain_diagnostic(
    assignment: &mut AssignedConsumerAssignmentState,
    fault: Option<kafka_client_engine::AssignedConsumerAcceptedFaultKind>,
) {
    assignment.retain_control_diagnostic(fault.map(translate_assigned_assignment_fault));
}
