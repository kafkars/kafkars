//! Exact owner retention when post-admission preparation cannot execute.

use kafka_client_core::{
    GroupOffsetCommitEffect, GroupOffsetCommitInput, GroupOffsetCommitMachine,
    GroupOffsetCommitTerminal, OperationId,
};

use super::{
    host::{
        GROUP_OFFSET_COMMIT_OPERATION_BYTES, GroupOffsetCommitHost, GroupOffsetCommitHostError,
        GroupOffsetCommitPreparationFault, GroupOffsetCommitSubmission,
    },
    preparation::{HostPreparation, InstalledPreparation, PreparationOutcome},
};

impl GroupOffsetCommitHost {
    pub(super) fn settle_preparation_failure(
        &mut self,
        operation_id: OperationId,
        machine: &mut GroupOffsetCommitMachine,
        preparation: HostPreparation,
        fault: GroupOffsetCommitHostError,
    ) -> PreparationOutcome {
        match self.engine_failure_terminal(machine, operation_id) {
            Ok(terminal) => PreparationOutcome::Installed(InstalledPreparation {
                attempt: None,
                terminal: Some(terminal),
                byte_charge: GROUP_OFFSET_COMMIT_OPERATION_BYTES,
                fault: Some(fault),
            }),
            Err(error) => PreparationOutcome::RetainedFault { error, preparation },
        }
    }

    pub(super) fn retain_preparation_fault(&mut self, preparation: HostPreparation) {
        self.preparation_fault = Some(match preparation {
            HostPreparation::Ready { prepared, request } => {
                GroupOffsetCommitPreparationFault::Ready(GroupOffsetCommitSubmission {
                    prepared,
                    request,
                })
            }
            HostPreparation::Fault { error, request } => {
                GroupOffsetCommitPreparationFault::Protocol { error, request }
            }
        });
    }

    fn engine_failure_terminal(
        &mut self,
        machine: &mut GroupOffsetCommitMachine,
        expected_operation_id: OperationId,
    ) -> Result<GroupOffsetCommitTerminal, GroupOffsetCommitHostError> {
        let transition = machine
            .apply(GroupOffsetCommitInput::ExecutionUnavailable)
            .map_err(|error| GroupOffsetCommitHostError::Machine(error.kind()))?;
        match transition.into_effect() {
            Some(GroupOffsetCommitEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == expected_operation_id => Ok(terminal),
            Some(effect) => {
                self.effect_fault = Some(effect);
                Err(GroupOffsetCommitHostError::UnexpectedEffect)
            }
            None => Err(GroupOffsetCommitHostError::UnexpectedEffect),
        }
    }
}
