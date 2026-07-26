//! Registry reconciliation of position RPC ownership after driver teardown.

use kafka_client_core::{GroupPositionFence, Moment};

use crate::driver::{
    GroupPositionOffsetFetchCompletionRecovery, GroupPositionOffsetFetchKey,
    GroupPositionOffsetFetchTerminal,
};

use super::super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_position::{
        ClassicGroupPositionExecutionError, ClassicGroupPositionRecoveryFault,
    },
    registry::GroupConsumerRegistry,
};

enum NextPositionRecovery {
    Key(GroupPositionOffsetFetchKey),
    Terminal(GroupPositionOffsetFetchTerminal),
    Pending(GroupPositionFence),
    Completion(GroupPositionOffsetFetchCompletionRecovery),
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn recover_classic_group_positions_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ClassicGroupExecutionError> {
        if self.position_recovery_fault.is_some() {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
        if self.position_shutdown_recovery.is_none() {
            let Some(mut calls) = self.position_calls.take() else {
                return Ok(());
            };
            self.position_shutdown_recovery =
                Some(calls.recover_group_position_offset_fetches_after_driver_shutdown());
        }
        while let Some(recovered) = self.take_next_position_recovery() {
            if let Err(fault) = self.reconcile_position_recovery(recovered) {
                let error = fault.error();
                self.position_recovery_fault = Some(fault);
                return Err(ClassicGroupExecutionError::Position(error));
            }
        }
        self.position_shutdown_recovery = None;
        Ok(())
    }

    fn take_next_position_recovery(&mut self) -> Option<NextPositionRecovery> {
        let recovery = self.position_shutdown_recovery.as_mut()?;
        if let Some(key) = recovery.pop_active() {
            return Some(NextPositionRecovery::Key(key));
        }
        if let Some(terminal) = recovery.take_settled() {
            return Some(NextPositionRecovery::Terminal(terminal));
        }
        if let Some(completion) = recovery.take_completion() {
            return Some(NextPositionRecovery::Completion(completion));
        }
        let fence = recovery.pending_fence()?;
        recovery.clear_pending_fence();
        Some(NextPositionRecovery::Pending(fence))
    }

    #[expect(
        clippy::result_large_err,
        reason = "reconciliation failure retains the exact recovered owner without allocation"
    )]
    fn reconcile_position_recovery(
        &mut self,
        recovered: NextPositionRecovery,
    ) -> Result<(), ClassicGroupPositionRecoveryFault> {
        match recovered {
            NextPositionRecovery::Key(key) => {
                let fence = key.fence();
                let Some(entry) = self
                    .entries
                    .iter_mut()
                    .find(|entry| entry.position.settlement_fence() == Some(fence))
                else {
                    return Err(ClassicGroupPositionRecoveryFault::missing_key(
                        ClassicGroupPositionExecutionError::TerminalCorrelation,
                        key,
                    ));
                };
                entry
                    .position
                    .recover_key_after_driver_shutdown(key, Moment::from_tick(u64::MAX))
            }
            NextPositionRecovery::Terminal(terminal) => {
                let fence = terminal.key().fence();
                let Some(entry) = self
                    .entries
                    .iter_mut()
                    .find(|entry| entry.position.settlement_fence() == Some(fence))
                else {
                    return Err(ClassicGroupPositionRecoveryFault::missing_terminal(
                        ClassicGroupPositionExecutionError::TerminalCorrelation,
                        terminal,
                    ));
                };
                entry
                    .position
                    .recover_terminal_after_driver_shutdown(terminal, Moment::from_tick(u64::MAX))
            }
            NextPositionRecovery::Pending(fence) => {
                let Some(entry) = self
                    .entries
                    .iter_mut()
                    .find(|entry| entry.position.settlement_fence() == Some(fence))
                else {
                    return Err(ClassicGroupPositionRecoveryFault::missing_fence(
                        ClassicGroupPositionExecutionError::TerminalCorrelation,
                        fence,
                    ));
                };
                entry
                    .position
                    .recover_confirmation_after_driver_shutdown(fence)
            }
            NextPositionRecovery::Completion(completion) => {
                let (key, observation) = completion.into_parts();
                let fence = observation.fence();
                let Some(entry) = self
                    .entries
                    .iter_mut()
                    .find(|entry| entry.position.settlement_fence() == Some(fence))
                else {
                    return Err(ClassicGroupPositionRecoveryFault::missing_completion(
                        ClassicGroupPositionExecutionError::TerminalCorrelation,
                        key,
                        observation,
                    ));
                };
                entry
                    .position
                    .recover_key_after_driver_shutdown(key, Moment::from_tick(u64::MAX))
                    .map_err(|fault| fault.with_completion(observation))
            }
        }
    }
}
