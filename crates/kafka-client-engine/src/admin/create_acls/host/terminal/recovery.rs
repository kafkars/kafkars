//! Exact accepted-call recovery after the unique driver has been destroyed.

use kafka_client_core::{
    CreateAclsEffect, CreateAclsInput, CreateAclsState, DeliveryStatus, Moment,
};

use super::super::{CreateAclsHandoff, CreateAclsHost, CreateAclsHostError};

impl CreateAclsHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), CreateAclsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (CreateAclsState::Ready, _) => self.apply(
                    operation_id,
                    CreateAclsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (CreateAclsState::AwaitingDriver, CreateAclsHandoff::Untouched) => {
                    self.apply(operation_id, CreateAclsInput::DriverRejected)?;
                }
                (CreateAclsState::AwaitingDriver, CreateAclsHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, CreateAclsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (CreateAclsState::Submitted, CreateAclsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (CreateAclsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(CreateAclsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), CreateAclsHostError> {
        if self.operations[index].recovered_call.is_none() {
            let call = self.operations[index]
                .call
                .take()
                .ok_or(CreateAclsHostError::InvalidHandoff)?;
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let operation = &self.operations[index];
        let recovered = operation
            .recovered_call
            .as_ref()
            .ok_or(CreateAclsHostError::InvalidHandoff)?;
        let plan = operation
            .machine
            .plan()
            .ok_or(CreateAclsHostError::SubmissionMismatch)?;
        if !recovered.matches_evidence(plan, operation.request_limit, operation.result_limit) {
            return Err(CreateAclsHostError::SubmissionMismatch);
        }
        Ok(())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), CreateAclsHostError> {
        {
            let operation = &self.operations[index];
            let recovered = operation
                .recovered_call
                .as_ref()
                .ok_or(CreateAclsHostError::InvalidHandoff)?;
            let plan = operation
                .machine
                .plan()
                .ok_or(CreateAclsHostError::SubmissionMismatch)?;
            if !recovered.matches_evidence(plan, operation.request_limit, operation.result_limit) {
                return Err(CreateAclsHostError::SubmissionMismatch);
            }
        }
        let transition =
            self.operations[index]
                .machine
                .apply(CreateAclsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(CreateAclsHostError::MissingTerminal)?;
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(CreateAclsHostError::InvalidHandoff)?;
        recovered.seal();
        match effect {
            CreateAclsEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => {
                self.operations[index].terminal = Some(terminal);
                self.publish_terminal(index)
            }
            _ => Err(CreateAclsHostError::MissingTerminal),
        }
    }
}
