//! Exact controller-attempt recovery after unique driver destruction.

use kafka_client_core::{
    DeliveryStatus, ListPartitionReassignmentsEffect, ListPartitionReassignmentsInput,
    ListPartitionReassignmentsState, Moment,
};

use super::super::{
    ListPartitionReassignmentsHandoff, ListPartitionReassignmentsHost,
    ListPartitionReassignmentsHostError,
};

impl ListPartitionReassignmentsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.prepare_raw(0)?;
                self.operations[0]
                    .raw_terminal
                    .as_mut()
                    .ok_or(ListPartitionReassignmentsHostError::MissingTerminal)?
                    .discard_controller_refresh_after_driver_shutdown();
                self.settle_raw(0)?;
                continue;
            }
            if operation.rejected_submission.is_some() {
                return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (ListPartitionReassignmentsState::Ready, _) => self.apply(
                    operation_id,
                    ListPartitionReassignmentsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    ListPartitionReassignmentsState::AwaitingDriver,
                    ListPartitionReassignmentsHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    ListPartitionReassignmentsInput::DriverRejected,
                )?,
                (
                    ListPartitionReassignmentsState::AwaitingDriver,
                    ListPartitionReassignmentsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        ListPartitionReassignmentsInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    ListPartitionReassignmentsState::Submitted,
                    ListPartitionReassignmentsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (ListPartitionReassignmentsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(ListPartitionReassignmentsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(ListPartitionReassignmentsHostError::InvalidHandoff)?;
            match call.recover_after_driver_shutdown() {
                Ok(recovered) => self.operations[index].recovered_call = Some(recovered),
                Err(call) => {
                    self.operations[index].call = Some(call);
                    return Err(ListPartitionReassignmentsHostError::InvalidHandoff);
                }
            }
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(ListPartitionReassignmentsHostError::InvalidHandoff)?;
        if operation.matches_recovered(recovered) {
            Ok(())
        } else {
            Err(ListPartitionReassignmentsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), ListPartitionReassignmentsHostError> {
        let operation = self
            .operations
            .get(index)
            .ok_or(ListPartitionReassignmentsHostError::UnknownOperation)?;
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(ListPartitionReassignmentsHostError::InvalidHandoff)?;
        if !operation.matches_recovered(recovered) {
            return Err(ListPartitionReassignmentsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            ListPartitionReassignmentsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(ListPartitionReassignmentsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(ListPartitionReassignmentsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(ListPartitionReassignmentsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
