//! One raw position terminal settlement or exact route confirmation turn.

use kafka_client_core::Moment;

use crate::driver::{
    GroupPositionOffsetFetchPoll, GroupPositionOffsetFetchTerminal,
    TrackedGroupPositionOffsetFetchCalls,
};

use super::super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_position::ClassicGroupPositionExecutionState, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

/// Outcome of attempting at most one ready position settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionSettlementTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn settle_one_classic_group_position(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupPositionSettlementTurn, ClassicGroupExecutionError> {
        let poll = self
            .position_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::PositionCallsUnavailable)?
            .poll_group_position_offset_fetch()
            .map_err(|_observation| ClassicGroupExecutionError::CallCompletion)?;
        let fence = match poll {
            GroupPositionOffsetFetchPoll::Idle => {
                return Ok(ClassicGroupPositionSettlementTurn::Idle);
            }
            GroupPositionOffsetFetchPoll::TerminalReady { fence }
            | GroupPositionOffsetFetchPoll::ConfirmationPending { fence } => fence,
        };
        let index = self
            .entries
            .iter()
            .position(|entry| entry.position.settlement_fence() == Some(fence))
            .ok_or(ClassicGroupExecutionError::CallIdentityMismatch)?;
        let calls = self
            .position_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::PositionCallsUnavailable)?;
        let entry = &mut self.entries[index];
        match poll {
            GroupPositionOffsetFetchPoll::Idle => Ok(ClassicGroupPositionSettlementTurn::Idle),
            GroupPositionOffsetFetchPoll::ConfirmationPending { .. } => {
                entry
                    .position
                    .confirm_terminal_settlement(calls)
                    .map_err(ClassicGroupExecutionError::Position)?;
                Ok(ClassicGroupPositionSettlementTurn::Progress)
            }
            GroupPositionOffsetFetchPoll::TerminalReady { .. } => {
                let terminal = begin_settlement(entry, calls)?;
                settle_terminal(entry, calls, now, terminal)?;
                Ok(ClassicGroupPositionSettlementTurn::Progress)
            }
        }
    }
}

fn begin_settlement(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedGroupPositionOffsetFetchCalls,
) -> Result<GroupPositionOffsetFetchTerminal, ClassicGroupExecutionError> {
    let ClassicGroupPositionExecutionState::DriverOwned(owner) = entry.position.state() else {
        return Err(ClassicGroupExecutionError::CallIdentityMismatch);
    };
    calls
        .begin_group_position_offset_fetch_settlement(owner.accepted())
        .map_err(|_error| ClassicGroupExecutionError::CallIdentityMismatch)
}

fn settle_terminal(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedGroupPositionOffsetFetchCalls,
    now: Moment,
    terminal: GroupPositionOffsetFetchTerminal,
) -> Result<(), ClassicGroupExecutionError> {
    match entry.position.apply_raw_terminal(&terminal, now) {
        Ok(()) => {
            drop(terminal);
            Ok(())
        }
        Err(failure) if failure.raw_terminal_is_restorable() => {
            let error = failure.error();
            restore_terminal(entry, calls, terminal)?;
            Err(ClassicGroupExecutionError::Position(error))
        }
        Err(failure) => {
            let error = failure.error();
            entry.fault =
                Some(ClassicGroupEntryFault::PositionTerminalPostCore { failure, terminal });
            Err(ClassicGroupExecutionError::Position(error))
        }
    }
}

fn restore_terminal(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedGroupPositionOffsetFetchCalls,
    terminal: GroupPositionOffsetFetchTerminal,
) -> Result<(), ClassicGroupExecutionError> {
    match calls.restore_group_position_offset_fetch_settlement(terminal) {
        Ok(()) => Ok(()),
        Err(failure) => {
            entry.fault = Some(ClassicGroupEntryFault::PositionTerminalRestore(failure));
            Err(ClassicGroupExecutionError::CallIdentityMismatch)
        }
    }
}
