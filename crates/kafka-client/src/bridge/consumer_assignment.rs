//! Deadline-first conversion and admission of facade-owned direct assignments.

use std::time::Duration;

use kafka_client_engine::{
    AssignedConsumerAssignment as EngineAssignment,
    AssignedConsumerAssignmentEpoch as EngineAssignmentEpoch,
    AssignedConsumerHandle as EngineAssignedConsumerHandle,
    AssignedConsumerStartPosition as EngineStartPosition,
};

use crate::{
    KafkaError,
    consumer::{StartPosition, TopicPartition},
};

use super::consumer_assignment_result::{
    translate_assigned_assignment_admission, translate_assigned_assignment_fault,
    translate_assigned_assignment_input,
};

/// Private accepted assignment fence retained for later consumer operations.
pub(crate) struct AssignedConsumerAssignmentState {
    epoch: EngineAssignmentEpoch,
    accepted_diagnostic: Option<KafkaError>,
}

impl AssignedConsumerAssignmentState {
    pub(crate) fn try_replace<I>(
        handle: &mut EngineAssignedConsumerHandle,
        entries: I,
        resolution_timeout: Duration,
    ) -> Result<Self, KafkaError>
    where
        I: IntoIterator<Item = TopicPartition>,
    {
        let capture = handle
            .capture_replace_assignment(resolution_timeout)
            .map_err(translate_assigned_assignment_admission)?;
        let entries = entries
            .into_iter()
            .map(into_engine_assignment)
            .collect::<Result<Vec<_>, _>>()?;
        let accepted = capture
            .try_replace_assignment(entries)
            .map_err(translate_assigned_assignment_admission)?;
        Ok(Self {
            epoch: accepted.epoch(),
            accepted_diagnostic: accepted.fault().map(translate_assigned_assignment_fault),
        })
    }
}

impl std::fmt::Debug for AssignedConsumerAssignmentState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerAssignmentState")
            .field("epoch", &self.epoch.get())
            .field("accepted_diagnostic", &self.accepted_diagnostic)
            .finish()
    }
}

pub(super) fn into_engine_assignment(
    entry: TopicPartition,
) -> Result<EngineAssignment, KafkaError> {
    let (topic, partition, start) = entry.into_parts();
    let start = start.ok_or_else(|| {
        KafkaError::new(
            crate::ErrorKind::Configuration,
            "assigned topic-partition requires an explicit start position",
        )
    })?;
    EngineAssignment::try_new(topic, partition, engine_start(start))
        .map_err(translate_assigned_assignment_input)
}

const fn engine_start(start: StartPosition) -> EngineStartPosition {
    match start {
        StartPosition::Beginning => EngineStartPosition::Beginning,
        StartPosition::End => EngineStartPosition::End,
        StartPosition::Offset(offset) => EngineStartPosition::Offset(offset),
    }
}
