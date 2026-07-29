//! Exact accepted-call recovery and settlement after unique driver destruction.

use kafka_client_core::{
    AdminListConsumerGroupsEffect, AdminListConsumerGroupsInput, AdminListConsumerGroupsState,
    DeliveryStatus, Moment,
};

use super::super::{
    ListConsumerGroupsHandoff, ListConsumerGroupsHost, ListConsumerGroupsHostError,
};

impl ListConsumerGroupsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListConsumerGroupsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            if operation.rejected_submission.is_some() {
                return Err(ListConsumerGroupsHostError::SubmissionMismatch);
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AdminListConsumerGroupsState::Ready, _) => self.apply(
                    operation_id,
                    AdminListConsumerGroupsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AdminListConsumerGroupsState::AwaitingDiscoveryDriver
                    | AdminListConsumerGroupsState::AwaitingBrokerDriver,
                    ListConsumerGroupsHandoff::Untouched,
                ) => self.apply(operation_id, AdminListConsumerGroupsInput::DriverRejected)?,
                (
                    AdminListConsumerGroupsState::AwaitingDiscoveryDriver
                    | AdminListConsumerGroupsState::AwaitingBrokerDriver,
                    ListConsumerGroupsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AdminListConsumerGroupsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AdminListConsumerGroupsState::DiscoverySubmitted
                    | AdminListConsumerGroupsState::BrokerSubmitted,
                    ListConsumerGroupsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminListConsumerGroupsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ListConsumerGroupsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), ListConsumerGroupsHostError> {
        let operation = self
            .operations
            .get_mut(index)
            .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
        let call = operation
            .call
            .as_mut()
            .ok_or(ListConsumerGroupsHostError::InvalidHandoff)?;
        if !call.recover_after_driver_shutdown() {
            return Err(ListConsumerGroupsHostError::InvalidHandoff);
        }
        if !operation.matches_call(
            operation
                .call
                .as_ref()
                .ok_or(ListConsumerGroupsHostError::InvalidHandoff)?,
        ) {
            return Err(ListConsumerGroupsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), ListConsumerGroupsHostError> {
        let operation = self
            .operations
            .get(index)
            .ok_or(ListConsumerGroupsHostError::UnknownOperation)?;
        let call = operation
            .call
            .as_ref()
            .ok_or(ListConsumerGroupsHostError::InvalidHandoff)?;
        if !call.is_recovered() || !operation.matches_call(call) {
            return Err(ListConsumerGroupsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            AdminListConsumerGroupsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(AdminListConsumerGroupsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ListConsumerGroupsHostError::MissingTerminal),
        };
        let call = self.operations[index]
            .call
            .take()
            .ok_or(ListConsumerGroupsHostError::InvalidHandoff)?;
        call.seal_recovered();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
