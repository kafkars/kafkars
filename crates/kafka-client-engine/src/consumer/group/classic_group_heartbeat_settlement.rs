//! Nonblocking Heartbeat settlement, restoration, and route confirmation.

use kafka_client_core::Moment;

use crate::driver::classic_group::{
    ClassicHeartbeatPoll, ClassicHeartbeatTerminal, TrackedClassicHeartbeatCalls,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat::{
        ClassicHeartbeatDriverOwner, ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor,
    },
    classic_group_heartbeat_interpret::{
        ClassicHeartbeatInterpretationFailure, interpret_heartbeat,
    },
    classic_group_heartbeat_prepare::map_revocation_kind,
    classic_group_rediscovery_transfer::confirm_heartbeat_rediscovery,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicHeartbeatSettlementTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn settle_one_classic_heartbeat(
        &mut self,
        now: Moment,
    ) -> Result<ClassicHeartbeatSettlementTurn, ClassicGroupExecutionError> {
        let poll = self
            .heartbeat_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
            .poll_classic_heartbeat()
            .map_err(|_observation| ClassicGroupExecutionError::CallCompletion)?;
        let key = match poll {
            ClassicHeartbeatPoll::Idle => return Ok(ClassicHeartbeatSettlementTurn::Idle),
            ClassicHeartbeatPoll::TerminalReady { key }
            | ClassicHeartbeatPoll::ConfirmationPending { key } => key,
        };
        let index = self
            .entries
            .iter()
            .position(|entry| entry.group_id() == key.group_id())
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        let calls = self
            .heartbeat_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
        let entry = &mut self.entries[index];
        let accepted = entry
            .heartbeat
            .accepted()
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        if accepted.key() != key {
            return Err(ClassicGroupExecutionError::CallIdentityMismatch);
        }
        if matches!(poll, ClassicHeartbeatPoll::ConfirmationPending { .. }) {
            if entry.rediscovery.awaits_route_transfer() {
                let permit = self
                    .coordinator_invalidations
                    .as_mut()
                    .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?
                    .try_reserve(key.group_id())
                    .map_err(|_error| ClassicGroupExecutionError::CoordinatorInvalidationReserve)?;
                confirm_heartbeat_rediscovery(entry, calls, permit)?;
            } else {
                confirm_heartbeat(entry, calls)?;
            }
            return Ok(ClassicHeartbeatSettlementTurn::Progress);
        }
        if !matches!(
            entry.heartbeat.state(),
            ClassicHeartbeatExecutionState::DriverOwned(_)
        ) {
            return Err(ClassicGroupExecutionError::HeartbeatState);
        }
        let terminal = calls
            .begin_classic_heartbeat_settlement(accepted)
            .map_err(|_error| ClassicGroupExecutionError::CallIdentityMismatch)?;
        settle_terminal(entry, calls, now, terminal)?;
        Ok(ClassicHeartbeatSettlementTurn::Progress)
    }
}

fn settle_terminal(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedClassicHeartbeatCalls,
    now: Moment,
    terminal: ClassicHeartbeatTerminal,
) -> Result<(), ClassicGroupExecutionError> {
    match interpret_heartbeat(entry, now, &terminal) {
        Ok(successor) => {
            if let Err(kind) = stage_confirmation(entry, successor) {
                entry.fault = Some(ClassicGroupEntryFault::HeartbeatPostCore(terminal));
                return Err(kind);
            }
            drop(terminal);
            Ok(())
        }
        Err(ClassicHeartbeatInterpretationFailure::Restorable(kind)) => {
            restore_terminal(entry, calls, terminal)?;
            Err(kind)
        }
        Err(ClassicHeartbeatInterpretationFailure::PostCore(kind)) => {
            entry.fault = Some(ClassicGroupEntryFault::HeartbeatPostCore(terminal));
            Err(kind)
        }
        Err(ClassicHeartbeatInterpretationFailure::PostCoreRejection(rejection)) => {
            entry.fault = Some(ClassicGroupEntryFault::HeartbeatRejectionPostCore {
                rejection,
                terminal,
            });
            Err(ClassicGroupExecutionError::RejoinPostCore)
        }
        Err(ClassicHeartbeatInterpretationFailure::Revoke(failure)) => {
            let kind = failure.kind;
            entry.fault =
                Some(ClassicGroupEntryFault::HeartbeatTerminalRevoke { failure, terminal });
            Err(map_revocation_kind(kind))
        }
    }
}

fn stage_confirmation(
    entry: &mut GroupConsumerEntry,
    successor: ClassicHeartbeatSuccessor,
) -> Result<(), ClassicGroupExecutionError> {
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::DriverOwned(owner) = state else {
        entry.heartbeat.set(state);
        return Err(ClassicGroupExecutionError::HeartbeatState);
    };
    entry
        .heartbeat
        .set(ClassicHeartbeatExecutionState::ConfirmationPending { owner, successor });
    Ok(())
}

fn confirm_heartbeat(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedClassicHeartbeatCalls,
) -> Result<(), ClassicGroupExecutionError> {
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::ConfirmationPending { owner, successor } = state else {
        entry.heartbeat.set(state);
        return Err(ClassicGroupExecutionError::HeartbeatState);
    };
    match calls.confirm_classic_heartbeat_settlement(owner.into_accepted()) {
        Ok(()) => {
            entry.heartbeat.set(successor.into_state());
            Ok(())
        }
        Err(failure) => {
            let (accepted, _error) = failure.into_parts();
            entry
                .heartbeat
                .set(ClassicHeartbeatExecutionState::ConfirmationPending {
                    owner: ClassicHeartbeatDriverOwner::new(accepted),
                    successor,
                });
            Err(ClassicGroupExecutionError::CallIdentityMismatch)
        }
    }
}

fn restore_terminal(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedClassicHeartbeatCalls,
    terminal: ClassicHeartbeatTerminal,
) -> Result<(), ClassicGroupExecutionError> {
    match calls.restore_classic_heartbeat_settlement(terminal) {
        Ok(()) => Ok(()),
        Err(failure) => {
            entry.fault = Some(ClassicGroupEntryFault::HeartbeatTerminal(failure));
            Err(ClassicGroupExecutionError::CallIdentityMismatch)
        }
    }
}
