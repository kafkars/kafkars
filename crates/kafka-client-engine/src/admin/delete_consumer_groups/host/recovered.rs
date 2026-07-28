//! Exact destructive-call recovery after the unique driver is destroyed.

use kafka_client_core::{
    DeleteConsumerGroupsInput, DeleteConsumerGroupsState, DeliveryStatus, Moment,
};

use super::{DeleteConsumerGroupsHandoff, DeleteConsumerGroupsHost, DeleteConsumerGroupsHostError};

impl DeleteConsumerGroupsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.rejected_submission.is_some() {
                return Err(DeleteConsumerGroupsHostError::SubmissionMismatch);
            }
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (DeleteConsumerGroupsState::Ready, _) => self.apply(
                    operation_id,
                    DeleteConsumerGroupsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DeleteConsumerGroupsState::AwaitingDriver,
                    DeleteConsumerGroupsHandoff::Untouched,
                ) => self.apply(operation_id, DeleteConsumerGroupsInput::DriverRejected)?,
                (
                    DeleteConsumerGroupsState::AwaitingDriver,
                    DeleteConsumerGroupsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DeleteConsumerGroupsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteConsumerGroupsState::Submitted, DeleteConsumerGroupsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DeleteConsumerGroupsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DeleteConsumerGroupsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DeleteConsumerGroupsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(DeleteConsumerGroupsHostError::InvalidHandoff)?;
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.validate_recovered(index)
    }

    fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DeleteConsumerGroupsHostError> {
        self.validate_recovered(index)?;
        let transition =
            self.operations[index]
                .machine
                .apply(DeleteConsumerGroupsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(DeleteConsumerGroupsHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DeleteConsumerGroupsHostError::InvalidHandoff)?;
        recovered.seal();
        self.install_effect(index, effect)
    }

    fn validate_recovered(&self, index: usize) -> Result<(), DeleteConsumerGroupsHostError> {
        let operation = &self.operations[index];
        let target = operation
            .machine
            .current_target()
            .ok_or(DeleteConsumerGroupsHostError::SubmissionMismatch)?;
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(DeleteConsumerGroupsHostError::InvalidHandoff)?;
        if recovered.matches_evidence(
            &operation.plan,
            target,
            operation.request_limit,
            operation.result_limit,
        ) {
            Ok(())
        } else {
            Err(DeleteConsumerGroupsHostError::SubmissionMismatch)
        }
    }
}
