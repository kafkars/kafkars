//! Exact accepted-call handoff and post-driver coordinator recovery.

use kafka_client_core::{
    AdminDescribeConsumerGroupsCallKind, AdminDescribeConsumerGroupsEffect,
    AdminDescribeConsumerGroupsInput, AdminDescribeConsumerGroupsState, DeliveryStatus, Moment,
    OperationId,
};

use crate::driver::DescribeConsumerGroupsCall;

use super::{
    DescribeConsumerGroupsHandoff, DescribeConsumerGroupsHost, DescribeConsumerGroupsHostError,
};

impl DescribeConsumerGroupsHost {
    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: DescribeConsumerGroupsCall,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeConsumerGroupsHostError::UnknownOperation)?;
        if self.operations[index].handoff != DescribeConsumerGroupsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(DescribeConsumerGroupsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let call = self.operations[index]
            .call
            .as_ref()
            .ok_or(DescribeConsumerGroupsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_call(call) {
            return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            AdminDescribeConsumerGroupsInput::DriverAccepted,
        )
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "rejected handoff consumes the exact owned coordinator query evidence"
    )]
    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        group_id: String,
        include_authorized_operations: bool,
        call_kind: AdminDescribeConsumerGroupsCallKind,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(DescribeConsumerGroupsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != DescribeConsumerGroupsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(DescribeConsumerGroupsHostError::InvalidHandoff);
        }
        if operation.machine.state() != AdminDescribeConsumerGroupsState::AwaitingDriver
            || !operation.matches_evidence(
                &group_id,
                include_authorized_operations,
                call_kind,
                request_scratch_limit,
                result_limit,
            )
        {
            return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
        }
        self.apply(
            operation_id,
            AdminDescribeConsumerGroupsInput::DriverRejected,
        )
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AdminDescribeConsumerGroupsState::Ready, _) => self.apply(
                    operation_id,
                    AdminDescribeConsumerGroupsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AdminDescribeConsumerGroupsState::AwaitingDriver,
                    DescribeConsumerGroupsHandoff::Untouched,
                ) => self.apply(
                    operation_id,
                    AdminDescribeConsumerGroupsInput::DriverRejected,
                )?,
                (
                    AdminDescribeConsumerGroupsState::AwaitingDriver,
                    DescribeConsumerGroupsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        AdminDescribeConsumerGroupsInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AdminDescribeConsumerGroupsState::Submitted,
                    DescribeConsumerGroupsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminDescribeConsumerGroupsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DescribeConsumerGroupsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeConsumerGroupsHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(DescribeConsumerGroupsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeConsumerGroupsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeConsumerGroupsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(DescribeConsumerGroupsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            AdminDescribeConsumerGroupsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(AdminDescribeConsumerGroupsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DescribeConsumerGroupsHostError::MissingTerminal),
        };
        self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeConsumerGroupsHostError::InvalidHandoff)?
            .seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
