//! Exact accepted-call and voter-plan recovery after driver destruction.

use kafka_client_core::{
    AddRaftVoterEffect, AddRaftVoterInput, AddRaftVoterState, DeliveryStatus, Moment,
};

use super::super::{AddRaftVoterHandoff, AddRaftVoterHost, AddRaftVoterHostError};

impl AddRaftVoterHost {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), AddRaftVoterHostError> {
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
                (AddRaftVoterState::Ready, _) => self.apply(
                    operation_id,
                    AddRaftVoterInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (AddRaftVoterState::AwaitingDriver, AddRaftVoterHandoff::Untouched) => {
                    self.apply(operation_id, AddRaftVoterInput::DriverRejected)?;
                }
                (AddRaftVoterState::AwaitingDriver, AddRaftVoterHandoff::HandedOff) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AddRaftVoterInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (AddRaftVoterState::Submitted, AddRaftVoterHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AddRaftVoterState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AddRaftVoterHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), AddRaftVoterHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AddRaftVoterHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AddRaftVoterHostError> {
        let transition =
            self.operations[index]
                .machine
                .apply(AddRaftVoterInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let effect = transition
            .into_effect()
            .ok_or(AddRaftVoterHostError::MissingTerminal)?;
        let terminal = match effect {
            AddRaftVoterEffect::Complete {
                operation_id,
                terminal,
            } if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AddRaftVoterHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AddRaftVoterHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
