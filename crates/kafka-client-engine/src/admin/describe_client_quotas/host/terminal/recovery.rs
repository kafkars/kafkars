//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, DescribeClientQuotasEffect, DescribeClientQuotasInput,
    DescribeClientQuotasState, Moment,
};

use super::super::{
    DescribeClientQuotasHandoff, DescribeClientQuotasHost, DescribeClientQuotasHostError,
};

impl DescribeClientQuotasHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeClientQuotasHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.rejected_submission.is_some() {
                return Err(DescribeClientQuotasHostError::SubmissionMismatch);
            }
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (DescribeClientQuotasState::Ready, _) => self.apply(
                    operation_id,
                    DescribeClientQuotasInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeClientQuotasState::AwaitingDriver,
                    DescribeClientQuotasHandoff::Untouched,
                ) => {
                    self.apply(operation_id, DescribeClientQuotasInput::DriverRejected)?;
                }
                (
                    DescribeClientQuotasState::AwaitingDriver,
                    DescribeClientQuotasHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DescribeClientQuotasInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeClientQuotasState::Submitted, DescribeClientQuotasHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeClientQuotasState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DescribeClientQuotasHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DescribeClientQuotasHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(DescribeClientQuotasHostError::InvalidHandoff)?;
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(DescribeClientQuotasHostError::InvalidHandoff)?;
        if !recovered.matches(
            &operation.plan,
            operation.request_scratch_limit,
            operation.result_limit,
        ) {
            return Err(DescribeClientQuotasHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeClientQuotasHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(DescribeClientQuotasHostError::InvalidHandoff)?;
            if !recovered.matches(
                &operation.plan,
                operation.request_scratch_limit,
                operation.result_limit,
            ) {
                return Err(DescribeClientQuotasHostError::SubmissionMismatch);
            }
        }
        let transition =
            self.operations[index]
                .machine
                .apply(DescribeClientQuotasInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(DescribeClientQuotasEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DescribeClientQuotasHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeClientQuotasHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
