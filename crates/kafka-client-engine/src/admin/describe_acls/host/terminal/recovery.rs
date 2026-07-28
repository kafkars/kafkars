//! Exact accepted-call recovery after unique driver destruction.

use kafka_client_core::{
    DeliveryStatus, DescribeAclsEffect, DescribeAclsInput, DescribeAclsState, Moment,
};

use super::super::{DescribeAclsHandoff, DescribeAclsHost, DescribeAclsHostError};

impl DescribeAclsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), DescribeAclsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (DescribeAclsState::Ready, _) => self.apply(
                    operation_id,
                    DescribeAclsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (DescribeAclsState::AwaitingDriver, DescribeAclsHandoff::Untouched) => {
                    self.apply(operation_id, DescribeAclsInput::DriverRejected)?;
                }
                (DescribeAclsState::AwaitingDriver, DescribeAclsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DescribeAclsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeAclsState::Submitted, DescribeAclsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeAclsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(DescribeAclsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), DescribeAclsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(DescribeAclsHostError::InvalidHandoff)?;
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(DescribeAclsHostError::InvalidHandoff)?;
        if operation.matches_recovered(recovered) {
            Ok(())
        } else {
            Err(DescribeAclsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeAclsHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(DescribeAclsHostError::InvalidHandoff)?;
            if !operation.matches_recovered(recovered) {
                return Err(DescribeAclsHostError::SubmissionMismatch);
            }
        }
        let transition =
            self.operations[index]
                .machine
                .apply(DescribeAclsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(DescribeAclsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DescribeAclsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeAclsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
