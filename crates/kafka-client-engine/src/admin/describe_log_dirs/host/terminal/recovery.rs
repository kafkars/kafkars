//! Exact multi-broker call recovery after the unique driver is destroyed.

use kafka_client_core::{
    AdminDescribeLogDirsInput, AdminDescribeLogDirsState, DeliveryStatus, Moment,
};

use super::super::{DescribeLogDirsHandoff, DescribeLogDirsHost, DescribeLogDirsHostError};

impl DescribeLogDirsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DescribeLogDirsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.rejected_submission.is_some() {
                return Err(DescribeLogDirsHostError::SubmissionMismatch);
            }
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AdminDescribeLogDirsState::Ready, _) => self.apply(
                    operation_id,
                    AdminDescribeLogDirsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (AdminDescribeLogDirsState::AwaitingDriver, DescribeLogDirsHandoff::Untouched) => {
                    self.apply(operation_id, AdminDescribeLogDirsInput::DriverRejected)?;
                }
                (AdminDescribeLogDirsState::AwaitingDriver, DescribeLogDirsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AdminDescribeLogDirsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminDescribeLogDirsState::Submitted, DescribeLogDirsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminDescribeLogDirsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DescribeLogDirsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DescribeLogDirsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(DescribeLogDirsHostError::InvalidHandoff)?;
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(DescribeLogDirsHostError::InvalidHandoff)?;
        let broker_id = operation
            .machine
            .current_broker()
            .ok_or(DescribeLogDirsHostError::SubmissionMismatch)?;
        if !recovered.matches(
            broker_id,
            operation.plan.selection(),
            operation.request_scratch_limit,
            operation.result_limit,
        ) {
            return Err(DescribeLogDirsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeLogDirsHostError> {
        self.validate_recovered(index)?;
        let transition =
            self.operations[index]
                .machine
                .apply(AdminDescribeLogDirsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(DescribeLogDirsHostError::MissingTerminal)?;
        self.validate_effect(index, &effect)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeLogDirsHostError::InvalidHandoff)?;
        recovered.seal();
        self.install_effect(index, effect)
    }

    fn validate_recovered(&self, index: usize) -> Result<(), DescribeLogDirsHostError> {
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(DescribeLogDirsHostError::InvalidHandoff)?;
        let broker_id = operation
            .machine
            .current_broker()
            .ok_or(DescribeLogDirsHostError::SubmissionMismatch)?;
        if recovered.matches(
            broker_id,
            operation.plan.selection(),
            operation.request_scratch_limit,
            operation.result_limit,
        ) {
            Ok(())
        } else {
            Err(DescribeLogDirsHostError::SubmissionMismatch)
        }
    }
}
