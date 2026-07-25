//! Provenance-aware replay after driver shutdown transfers exact ownership.

use kafka_client_core::{
    DeliveryStatus, GroupOffsetCommitInput, GroupOffsetCommitState, OperationId,
};

use super::host::{
    GroupOffsetCommitAttempt, GroupOffsetCommitHost, GroupOffsetCommitHostError,
    GroupOffsetCommitSettlementFault, GroupOffsetCommitSettlementProvenance,
};

impl GroupOffsetCommitHost {
    pub(super) fn replay_recovered_settlements(
        &mut self,
    ) -> Result<(), GroupOffsetCommitHostError> {
        loop {
            if self.settlement_fault.is_none()
                && let Some(settled) = self
                    .shutdown_recovery
                    .as_mut()
                    .and_then(crate::driver::GroupOffsetCommitShutdownRecovery::take_settled)
            {
                let (operation_id, input) = settled.into_parts();
                self.settlement_fault = Some(GroupOffsetCommitSettlementFault {
                    operation_id,
                    input,
                    provenance: GroupOffsetCommitSettlementProvenance::TransportOwned,
                });
            }
            if self.settlement_fault.is_none() {
                return Ok(());
            }
            self.replay_settlement_fault()?;
        }
    }

    fn replay_settlement_fault(&mut self) -> Result<(), GroupOffsetCommitHostError> {
        let operation_id = self
            .settlement_fault
            .as_ref()
            .map(|fault| fault.operation_id)
            .ok_or(GroupOffsetCommitHostError::Settlement)?;
        let index = self
            .operation_index(operation_id)
            .ok_or(GroupOffsetCommitHostError::UnknownOperation)?;
        let requires_driver_acceptance = self.operations[index].machine.state()
            == GroupOffsetCommitState::AwaitingDriver
            && self.settlement_fault.as_ref().is_some_and(|fault| {
                fault.provenance == GroupOffsetCommitSettlementProvenance::TransportOwned
                    && !matches!(&fault.input, GroupOffsetCommitInput::DriverAccepted)
            });
        if requires_driver_acceptance {
            self.apply_recovery_driver_acceptance(index)?;
        }
        let fault = self
            .settlement_fault
            .take()
            .ok_or(GroupOffsetCommitHostError::Settlement)?;
        let is_terminal = !matches!(&fault.input, GroupOffsetCommitInput::DriverAccepted);
        let provenance = fault.provenance;
        self.apply_recovery_input(fault.operation_id, fault.input, provenance)?;
        if is_terminal {
            self.release_recovered_attempt(index, provenance)?;
        }
        Ok(())
    }

    fn apply_recovery_driver_acceptance(
        &mut self,
        index: usize,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let transition = self.operations[index]
            .machine
            .apply(GroupOffsetCommitInput::DriverAccepted)
            .map_err(|error| {
                let fault = GroupOffsetCommitHostError::Machine(error.kind());
                self.fault = Some(fault);
                fault
            })?;
        if let Some(effect) = transition.into_effect() {
            self.effect_fault = Some(effect);
            self.fault = Some(GroupOffsetCommitHostError::UnexpectedEffect);
            return Err(GroupOffsetCommitHostError::UnexpectedEffect);
        }
        Ok(())
    }

    pub(super) fn recover_pending_confirmation(
        &mut self,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let Some(operation_id) = self
            .shutdown_recovery
            .as_ref()
            .and_then(crate::driver::GroupOffsetCommitShutdownRecovery::pending_operation_id)
        else {
            return Ok(());
        };
        let index = self
            .operation_index(operation_id)
            .ok_or(GroupOffsetCommitHostError::UnknownOperation)?;
        if self.operations[index].terminal.is_none() {
            return Err(GroupOffsetCommitHostError::Settlement);
        }
        self.operations[index].replace_attempt(None);
        if let Some(recovery) = self.shutdown_recovery.as_mut() {
            recovery.clear_pending_operation_id();
        }
        Ok(())
    }

    pub(super) fn settle_transport_owned_failure(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(GroupOffsetCommitHostError::UnknownOperation)?;
        if self.operations[index].machine.state() == GroupOffsetCommitState::AwaitingDriver {
            self.apply_nonterminal(index, GroupOffsetCommitInput::DriverAccepted)?;
        }
        self.apply_terminal(
            index,
            GroupOffsetCommitInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            GroupOffsetCommitSettlementProvenance::TransportOwned,
        )?;
        self.release_recovered_attempt(index, GroupOffsetCommitSettlementProvenance::TransportOwned)
    }

    fn release_recovered_attempt(
        &mut self,
        index: usize,
        provenance: GroupOffsetCommitSettlementProvenance,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let attempt = self.operations[index].replace_attempt(None);
        match (provenance, attempt) {
            (
                GroupOffsetCommitSettlementProvenance::DefinitelyUnsent,
                Some(GroupOffsetCommitAttempt::Queued(submission)),
            ) => {
                drop(submission);
                Ok(())
            }
            (
                GroupOffsetCommitSettlementProvenance::DefinitelyUnsent
                | GroupOffsetCommitSettlementProvenance::TransportOwned,
                Some(GroupOffsetCommitAttempt::Recovery(prepared)),
            ) => {
                drop(prepared);
                Ok(())
            }
            (
                GroupOffsetCommitSettlementProvenance::TransportOwned,
                Some(GroupOffsetCommitAttempt::HandedOff),
            ) => Ok(()),
            (_, attempt) => {
                self.operations[index].replace_attempt(attempt);
                Err(GroupOffsetCommitHostError::Settlement)
            }
        }
    }

    fn apply_recovery_input(
        &mut self,
        operation_id: OperationId,
        input: GroupOffsetCommitInput,
        provenance: GroupOffsetCommitSettlementProvenance,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(GroupOffsetCommitHostError::UnknownOperation)?;
        if matches!(&input, GroupOffsetCommitInput::DriverAccepted) {
            self.apply_nonterminal(index, input)
        } else {
            self.apply_terminal(index, input, provenance)
        }
    }
}
