//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, ListConsumerGroupOffsetsInput, ListConsumerGroupOffsetsState, Moment,
};

use super::super::{
    ListConsumerGroupOffsetsHandoff, ListConsumerGroupOffsetsHost,
    ListConsumerGroupOffsetsHostError,
};

impl ListConsumerGroupOffsetsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (ListConsumerGroupOffsetsState::Ready, _) => self.apply(
                    operation_id,
                    ListConsumerGroupOffsetsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    ListConsumerGroupOffsetsState::AwaitingDriver,
                    ListConsumerGroupOffsetsHandoff::Untouched,
                ) => self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverRejected)?,
                (
                    ListConsumerGroupOffsetsState::AwaitingDriver,
                    ListConsumerGroupOffsetsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, ListConsumerGroupOffsetsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    ListConsumerGroupOffsetsState::Submitted,
                    ListConsumerGroupOffsetsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (ListConsumerGroupOffsetsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ListConsumerGroupOffsetsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let plan = operation.active_plan()?;
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(ListConsumerGroupOffsetsHostError::InvalidHandoff)?;
        if !recovered.matches_evidence(plan, operation.remaining_result_bytes) {
            return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(in crate::admin::group_offsets) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), ListConsumerGroupOffsetsHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(ListConsumerGroupOffsetsHostError::InvalidHandoff)?;
            if !recovered
                .matches_evidence(operation.active_plan()?, operation.remaining_result_bytes)
            {
                return Err(ListConsumerGroupOffsetsHostError::SubmissionMismatch);
            }
        }
        let transition = self.operations[index].machine.apply(
            ListConsumerGroupOffsetsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let effect = transition
            .into_effect()
            .ok_or(ListConsumerGroupOffsetsHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(ListConsumerGroupOffsetsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].active_plan = None;
        self.install_effect(index, effect)
    }
}
