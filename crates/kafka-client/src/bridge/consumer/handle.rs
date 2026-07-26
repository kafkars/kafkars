//! Unique facade ownership over the engine's assigned-consumer capability.

use kafka_client_engine::{
    AssignedConsumerHandle as EngineAssignedConsumerHandle, Engine as SharedEngine,
};

use crate::KafkaError;

use super::{
    assignment::AssignedConsumerAssignmentState,
    batch::AssignedConsumerBatch,
    batch_result::translate_assigned_batch_observation,
    close::AssignedConsumerClose,
    control::{try_pause, try_resume_captured, try_seek_captured},
    control_result::{translate_assigned_control_admission, translate_missing_assignment},
    event::translate_assigned_event,
    event_result::translate_assigned_event_observation,
    next_event::AssignedConsumerNextEvent,
    recv::AssignedConsumerRecv,
    result::translate_assigned_consumer_claim,
};

/// Private linear bridge retaining the engine's sole assigned-consumer handle.
pub(crate) struct AssignedConsumerEngine {
    handle: EngineAssignedConsumerHandle,
    assignment: Option<AssignedConsumerAssignmentState>,
}

impl AssignedConsumerEngine {
    pub(crate) fn claim(engine: &SharedEngine) -> Result<Self, KafkaError> {
        engine
            .claim_assigned_consumer()
            .map(|handle| Self {
                handle,
                assignment: None,
            })
            .map_err(translate_assigned_consumer_claim)
    }

    /// Captures time before converting facade inputs, then attempts replacement.
    pub(crate) fn try_replace_assignment<I>(
        &mut self,
        entries: I,
        resolution_timeout: std::time::Duration,
    ) -> Result<(), KafkaError>
    where
        I: IntoIterator<Item = crate::consumer::TopicPartition>,
    {
        let assignment = AssignedConsumerAssignmentState::try_replace(
            &mut self.handle,
            entries,
            resolution_timeout,
        )?;
        self.assignment = Some(assignment);
        Ok(())
    }

    /// Attempts one deadline-free assignment-fenced pause.
    pub(crate) fn try_pause(
        &mut self,
        partition: &crate::consumer::TopicPartition,
    ) -> Result<(), KafkaError> {
        let assignment = self
            .assignment
            .as_mut()
            .ok_or_else(translate_missing_assignment)?;
        try_pause(&mut self.handle, assignment, partition)
    }

    /// Captures time before checking state or converting the facade target.
    pub(crate) fn try_resume(
        &mut self,
        partition: &crate::consumer::TopicPartition,
        resolution_timeout: std::time::Duration,
    ) -> Result<(), KafkaError> {
        let capture = self
            .handle
            .capture_resume(resolution_timeout)
            .map_err(translate_assigned_control_admission)?;
        let assignment = self
            .assignment
            .as_mut()
            .ok_or_else(translate_missing_assignment)?;
        try_resume_captured(capture, assignment, partition)
    }

    /// Captures time before checking state or converting facade seek values.
    pub(crate) fn try_seek(
        &mut self,
        partition: &crate::consumer::TopicPartition,
        position: crate::consumer::StartPosition,
        resolution_timeout: std::time::Duration,
    ) -> Result<(), KafkaError> {
        let capture = self
            .handle
            .capture_seek(resolution_timeout)
            .map_err(translate_assigned_control_admission)?;
        let assignment = self
            .assignment
            .as_mut()
            .ok_or_else(translate_missing_assignment)?;
        try_seek_captured(capture, assignment, partition, position)
    }

    /// Transfers one already-authorized delivery without starting Fetch work.
    pub(crate) fn try_take_batch(&mut self) -> Result<Option<AssignedConsumerBatch>, KafkaError> {
        self.handle
            .try_take_batch()
            .map(|batch| batch.map(AssignedConsumerBatch::from_engine))
            .map_err(translate_assigned_batch_observation)
    }

    /// Transfers one retained failure event without waiting or starting work.
    pub(crate) fn try_take_event(
        &mut self,
    ) -> Result<Option<crate::consumer::AssignedConsumerEvent>, KafkaError> {
        self.handle
            .try_take_event()
            .map(|event| event.map(translate_assigned_event))
            .map_err(translate_assigned_event_observation)
    }

    /// Waits only for one already-retained failure event.
    pub(crate) fn next_event(&mut self) -> AssignedConsumerNextEvent<'_> {
        AssignedConsumerNextEvent::from_engine(self.handle.next_event())
    }

    /// Waits only for an already-authorized delivery.
    pub(crate) fn recv(&mut self) -> AssignedConsumerRecv<'_> {
        AssignedConsumerRecv::from_engine(self.handle.recv())
    }

    /// Attempts bounded close without consuming this capability on rejection.
    pub(crate) fn try_close(&mut self) -> Result<AssignedConsumerClose, KafkaError> {
        AssignedConsumerClose::from_admission(self.handle.try_close())
    }
}

impl std::fmt::Debug for AssignedConsumerEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignedConsumerEngine")
            .field("assignment", &self.assignment)
            .finish_non_exhaustive()
    }
}
