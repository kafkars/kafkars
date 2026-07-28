//! Exact accepted-call handoff and driver-shutdown recovery.

use kafka_client_core::{
    AlterPartitionReassignmentsEffect, AlterPartitionReassignmentsInput,
    AlterPartitionReassignmentsPlan, AlterPartitionReassignmentsState, DeliveryStatus, Moment,
    OperationId,
};

use crate::driver::AlterPartitionReassignmentsCall;

use super::{
    AlterPartitionReassignmentsHandoff, AlterPartitionReassignmentsHost,
    AlterPartitionReassignmentsHostError,
};

impl AlterPartitionReassignmentsHost {
    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AlterPartitionReassignmentsCall,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterPartitionReassignmentsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterPartitionReassignmentsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AlterPartitionReassignmentsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let operation = &self.operations[index];
        if !operation.call.as_ref().is_some_and(|call| {
            call.matches_evidence(
                &operation.response_plan,
                operation.request_scratch_limit,
                operation.result_limit,
            )
        }) {
            return Err(AlterPartitionReassignmentsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            AlterPartitionReassignmentsInput::DriverAccepted,
        )?;
        self.operations[index].handoff = AlterPartitionReassignmentsHandoff::Submitted;
        Ok(())
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "rejected handoff returns ownership through the host boundary"
    )]
    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterPartitionReassignmentsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != AlterPartitionReassignmentsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(AlterPartitionReassignmentsHostError::InvalidHandoff);
        }
        if operation.response_plan != plan
            || operation.request_scratch_limit != request_scratch_limit
            || operation.result_limit != result_limit
        {
            return Err(AlterPartitionReassignmentsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            AlterPartitionReassignmentsInput::DriverRejected,
        )
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.prepare_raw(0)?;
                self.operations[0]
                    .raw_terminal
                    .as_mut()
                    .ok_or(AlterPartitionReassignmentsHostError::MissingTerminal)?
                    .discard_controller_refresh_after_driver_shutdown();
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AlterPartitionReassignmentsState::Ready, _) => self.apply(
                    operation_id,
                    AlterPartitionReassignmentsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AlterPartitionReassignmentsState::AwaitingDriver,
                    AlterPartitionReassignmentsHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    AlterPartitionReassignmentsInput::DriverRejected,
                )?,
                (
                    AlterPartitionReassignmentsState::AwaitingDriver,
                    AlterPartitionReassignmentsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        AlterPartitionReassignmentsInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AlterPartitionReassignmentsState::Submitted,
                    AlterPartitionReassignmentsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterPartitionReassignmentsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AlterPartitionReassignmentsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(AlterPartitionReassignmentsHostError::InvalidHandoff)?;
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(AlterPartitionReassignmentsHostError::InvalidHandoff)?;
        if !recovered.matches_evidence(
            &operation.response_plan,
            operation.request_scratch_limit,
            operation.result_limit,
        ) {
            return Err(AlterPartitionReassignmentsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AlterPartitionReassignmentsHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(AlterPartitionReassignmentsHostError::InvalidHandoff)?;
            if !recovered.matches_evidence(
                &operation.response_plan,
                operation.request_scratch_limit,
                operation.result_limit,
            ) {
                return Err(AlterPartitionReassignmentsHostError::SubmissionMismatch);
            }
        }
        let transition = self.operations[index].machine.apply(
            AlterPartitionReassignmentsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(AlterPartitionReassignmentsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AlterPartitionReassignmentsHostError::MissingTerminal),
        };
        self.operations[index]
            .recovered_call
            .take()
            .ok_or(AlterPartitionReassignmentsHostError::InvalidHandoff)?
            .seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
