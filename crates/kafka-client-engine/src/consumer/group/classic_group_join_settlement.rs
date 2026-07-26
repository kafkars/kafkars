//! One bounded Join terminal interpretation or exact confirmation action.

use kafka_client_core::Moment;

use crate::driver::classic_group::{JoinGroupPoll, JoinGroupTerminal};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupJoinSuccessor,
    classic_group_join_interpret::{JoinInterpretation, JoinInterpretationFailure, interpret_join},
    classic_group_rediscovery_transfer::confirm_join_rediscovery,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupJoinSettlementTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn settle_one_classic_join(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupJoinSettlementTurn, ClassicGroupExecutionError> {
        let poll = self
            .join_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
            .poll_join_group()
            .map_err(|_observation| ClassicGroupExecutionError::CallCompletion)?;
        let key = match poll {
            JoinGroupPoll::Idle => return Ok(ClassicGroupJoinSettlementTurn::Idle),
            JoinGroupPoll::TerminalReady { key } | JoinGroupPoll::ConfirmationPending { key } => {
                key
            }
        };
        let index = self
            .entries
            .iter()
            .position(|entry| entry.group_id() == key.group_id())
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        let calls = self
            .join_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
        let entry = &mut self.entries[index];
        let accepted = entry
            .execution
            .join_call()
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?
            .accepted();
        if accepted.key() != key {
            return Err(ClassicGroupExecutionError::CallIdentityMismatch);
        }
        if matches!(poll, JoinGroupPoll::ConfirmationPending { .. }) {
            if entry.rediscovery.awaits_route_transfer() {
                let permit = self
                    .coordinator_invalidations
                    .as_mut()
                    .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
                    .try_reserve(key.group_id())
                    .map_err(|_error| ClassicGroupExecutionError::CoordinatorInvalidationReserve)?;
                confirm_join_rediscovery(entry, calls, permit)?;
            } else {
                entry.execution.confirm_join(calls)?;
            }
            return Ok(ClassicGroupJoinSettlementTurn::Progress);
        }
        let terminal = match calls.begin_join_group_settlement(accepted) {
            Ok(terminal) => terminal,
            Err(_error) => return Err(ClassicGroupExecutionError::CallIdentityMismatch),
        };
        match interpret_join(entry, now, &terminal) {
            Ok(JoinInterpretation::Confirm(successor)) => {
                stage_successor(entry, calls, terminal, successor)
            }
            Err(JoinInterpretationFailure::Restore(error)) => {
                restore_terminal(entry, calls, terminal)?;
                Err(error)
            }
            Err(JoinInterpretationFailure::PostCore(error)) => {
                entry.fault = Some(ClassicGroupEntryFault::JoinPostCore(terminal));
                Err(error)
            }
            Err(JoinInterpretationFailure::PostCoreRejection(rejection)) => {
                entry.fault = Some(ClassicGroupEntryFault::JoinRejectionPostCore {
                    rejection,
                    terminal,
                });
                Err(ClassicGroupExecutionError::RejoinPostCore)
            }
        }
    }
}

fn stage_successor(
    entry: &mut GroupConsumerEntry,
    calls: &mut crate::driver::classic_group::TrackedJoinGroupCalls,
    terminal: JoinGroupTerminal,
    successor: ClassicGroupJoinSuccessor,
) -> Result<ClassicGroupJoinSettlementTurn, ClassicGroupExecutionError> {
    match entry.execution.stage_join_confirmation(successor) {
        Ok(()) => {
            drop(terminal);
            Ok(ClassicGroupJoinSettlementTurn::Progress)
        }
        Err((error, successor)) => {
            match calls.restore_join_group_settlement(terminal) {
                Ok(()) => {
                    entry.fault = Some(ClassicGroupEntryFault::JoinSuccessor(successor));
                }
                Err(failure) => {
                    entry.fault =
                        Some(ClassicGroupEntryFault::JoinSuccessorRestore { successor, failure });
                }
            }
            Err(error)
        }
    }
}

fn restore_terminal(
    entry: &mut GroupConsumerEntry,
    calls: &mut crate::driver::classic_group::TrackedJoinGroupCalls,
    terminal: JoinGroupTerminal,
) -> Result<(), ClassicGroupExecutionError> {
    match calls.restore_join_group_settlement(terminal) {
        Ok(()) => Ok(()),
        Err(failure) => {
            entry.fault = Some(ClassicGroupEntryFault::JoinTerminal(failure));
            Err(ClassicGroupExecutionError::CallIdentityMismatch)
        }
    }
}
