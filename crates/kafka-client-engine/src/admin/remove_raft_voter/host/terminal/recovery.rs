//! Exact accepted-call and voter-correlation recovery after driver destruction.

use kafka_client_core::{
    DeliveryStatus, Moment, RemoveRaftVoterEffect, RemoveRaftVoterInput, RemoveRaftVoterState,
};

use super::super::{RemoveRaftVoterHandoff, RemoveRaftVoterHost, RemoveRaftVoterHostError};

impl RemoveRaftVoterHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), RemoveRaftVoterHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (RemoveRaftVoterState::Ready, _) => self.apply(
                    operation_id,
                    RemoveRaftVoterInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (RemoveRaftVoterState::AwaitingDriver, RemoveRaftVoterHandoff::Untouched) => {
                    self.apply(operation_id, RemoveRaftVoterInput::DriverRejected)?;
                }
                (RemoveRaftVoterState::AwaitingDriver, RemoveRaftVoterHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, RemoveRaftVoterInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (RemoveRaftVoterState::Submitted, RemoveRaftVoterHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (RemoveRaftVoterState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(RemoveRaftVoterHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), RemoveRaftVoterHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(RemoveRaftVoterHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), RemoveRaftVoterHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(RemoveRaftVoterInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(RemoveRaftVoterEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(RemoveRaftVoterHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(RemoveRaftVoterHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
