//! Lossless core input application before driver route confirmation.

use kafka_client_core::{GroupOffsetCommitEffect, GroupOffsetCommitInput, OperationId};

#[cfg(test)]
use super::host::GroupOffsetCommitAttempt;
use super::host::{
    GroupOffsetCommitHost, GroupOffsetCommitHostError, GroupOffsetCommitSettlementFault,
    GroupOffsetCommitSettlementProvenance,
};

impl GroupOffsetCommitHost {
    pub(super) fn settle_driver_terminal(
        &mut self,
        operation_id: OperationId,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(GroupOffsetCommitHostError::UnknownOperation)?;
        let input = self
            .calls
            .begin_group_commit_settlement(operation_id)
            .map_err(|_error| GroupOffsetCommitHostError::Settlement)?;
        match self.operations[index].machine.apply(input) {
            Ok(transition) => {
                self.capture_terminal(index, transition.into_effect())?;
                if self
                    .calls
                    .confirm_group_commit_settlement(operation_id)
                    .is_err()
                {
                    self.fault = Some(GroupOffsetCommitHostError::Settlement);
                    return Err(GroupOffsetCommitHostError::Settlement);
                }
                self.operations[index].replace_attempt(None);
                self.publish_terminal(index)
            }
            Err(error) => {
                let kind = error.kind();
                if let Err(failure) = self
                    .calls
                    .restore_group_commit_settlement(operation_id, error.into_input())
                {
                    let (input, _restore_error) = failure.into_parts();
                    self.settlement_fault = Some(GroupOffsetCommitSettlementFault {
                        operation_id,
                        input,
                        provenance: GroupOffsetCommitSettlementProvenance::TransportOwned,
                    });
                    self.fault = Some(GroupOffsetCommitHostError::Settlement);
                    return Err(GroupOffsetCommitHostError::Settlement);
                }
                Err(GroupOffsetCommitHostError::Machine(kind))
            }
        }
    }

    pub(super) fn apply_nonterminal(
        &mut self,
        index: usize,
        input: GroupOffsetCommitInput,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let operation_id = self.operations[index].operation_id;
        let transition = match self.operations[index].machine.apply(input) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = error.kind();
                self.settlement_fault = Some(GroupOffsetCommitSettlementFault {
                    operation_id,
                    input: error.into_input(),
                    provenance: GroupOffsetCommitSettlementProvenance::TransportOwned,
                });
                self.fault = Some(GroupOffsetCommitHostError::Machine(kind));
                return Err(GroupOffsetCommitHostError::Machine(kind));
            }
        };
        if let Some(effect) = transition.into_effect() {
            self.effect_fault = Some(effect);
            self.fault = Some(GroupOffsetCommitHostError::UnexpectedEffect);
            return Err(GroupOffsetCommitHostError::UnexpectedEffect);
        }
        Ok(())
    }

    pub(super) fn apply_terminal(
        &mut self,
        index: usize,
        input: GroupOffsetCommitInput,
        provenance: GroupOffsetCommitSettlementProvenance,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let operation_id = self.operations[index].operation_id;
        let transition = match self.operations[index].machine.apply(input) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = error.kind();
                self.settlement_fault = Some(GroupOffsetCommitSettlementFault {
                    operation_id,
                    input: error.into_input(),
                    provenance,
                });
                self.fault = Some(GroupOffsetCommitHostError::Machine(kind));
                return Err(GroupOffsetCommitHostError::Machine(kind));
            }
        };
        self.capture_terminal(index, transition.into_effect())
    }

    fn capture_terminal(
        &mut self,
        index: usize,
        effect: Option<GroupOffsetCommitEffect>,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let expected = self.operations[index].operation_id;
        match effect {
            Some(GroupOffsetCommitEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == expected => {
                self.operations[index].replace_terminal(Some(terminal));
                Ok(())
            }
            Some(effect) => {
                self.effect_fault = Some(effect);
                self.fault = Some(GroupOffsetCommitHostError::UnexpectedEffect);
                Err(GroupOffsetCommitHostError::UnexpectedEffect)
            }
            None => Err(GroupOffsetCommitHostError::UnexpectedEffect),
        }
    }

    #[cfg(test)]
    pub(super) fn install_accepted_terminal_for_test(
        &mut self,
        operation_id: OperationId,
        input: GroupOffsetCommitInput,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(GroupOffsetCommitHostError::UnknownOperation)?;
        self.operations[index].replace_attempt(Some(GroupOffsetCommitAttempt::HandedOff));
        self.apply_nonterminal(index, GroupOffsetCommitInput::DriverAccepted)?;
        self.calls.install_settlement_for_test(operation_id, input);
        Ok(())
    }
}
