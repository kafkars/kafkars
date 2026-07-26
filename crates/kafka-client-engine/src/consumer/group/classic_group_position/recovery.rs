//! Exact position receipt and raw-terminal reconciliation after driver teardown.

use kafka_client_core::{
    GroupPositionBootstrapEffect, GroupPositionBootstrapFetchFailure, GroupPositionBootstrapInput,
    Moment,
};

use crate::driver::{GroupPositionOffsetFetchKey, GroupPositionOffsetFetchTerminal};

use super::{
    ClassicGroupPositionCompleted, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    ClassicGroupPositionRecoveryFault,
};

#[expect(
    clippy::result_large_err,
    reason = "shutdown reconciliation failures retain every exact driver-era owner without allocation"
)]
impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) fn recover_key_after_driver_shutdown(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        now: Moment,
    ) -> Result<(), ClassicGroupPositionRecoveryFault> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::DriverOwned(owner) = state else {
            self.set(state);
            return Err(ClassicGroupPositionRecoveryFault::key(
                ClassicGroupPositionExecutionError::NotDriverOwned,
                key,
            ));
        };
        let (mut machine, correlation, accepted, result_buffer) = owner.into_parts();
        let supplied = key.fence();
        let expected = machine.fence();
        if expected != supplied || accepted.fence() != supplied {
            self.set(ClassicGroupPositionExecutionState::DriverOwned(
                super::ClassicGroupPositionDriverOwned::new(
                    machine,
                    correlation,
                    accepted,
                    result_buffer,
                ),
            ));
            return Err(ClassicGroupPositionRecoveryFault::key(
                ClassicGroupPositionExecutionError::FenceMismatch { expected, supplied },
                key,
            ));
        }
        if machine.deadline() != key.operation_deadline().core() {
            self.set(ClassicGroupPositionExecutionState::DriverOwned(
                super::ClassicGroupPositionDriverOwned::new(
                    machine,
                    correlation,
                    accepted,
                    result_buffer,
                ),
            ));
            return Err(ClassicGroupPositionRecoveryFault::key(
                ClassicGroupPositionExecutionError::DeadlineMismatch,
                key,
            ));
        }
        let transition = match machine.apply(GroupPositionBootstrapInput::FetchFailed {
            fence: supplied,
            now,
            failure: GroupPositionBootstrapFetchFailure::Transport,
        }) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = error.kind();
                self.set(ClassicGroupPositionExecutionState::DriverOwned(
                    super::ClassicGroupPositionDriverOwned::new(
                        machine,
                        correlation,
                        accepted,
                        result_buffer,
                    ),
                ));
                return Err(ClassicGroupPositionRecoveryFault::key(
                    ClassicGroupPositionExecutionError::Core(kind),
                    key,
                ));
            }
        };
        match transition.into_effect() {
            Some(GroupPositionBootstrapEffect::Complete {
                fence,
                deadline,
                terminal,
            }) => {
                self.set(ClassicGroupPositionExecutionState::Complete(
                    ClassicGroupPositionCompleted::new(machine, terminal),
                ));
                drop((correlation, accepted, result_buffer));
                if fence != supplied {
                    return Err(ClassicGroupPositionRecoveryFault::key(
                        ClassicGroupPositionExecutionError::CompletionFence,
                        key,
                    ));
                }
                if deadline != key.operation_deadline().core() {
                    return Err(ClassicGroupPositionRecoveryFault::key(
                        ClassicGroupPositionExecutionError::CompletionDeadline,
                        key,
                    ));
                }
                drop(key);
                Ok(())
            }
            effect => Err(ClassicGroupPositionRecoveryFault::post_core(
                key,
                machine,
                correlation,
                accepted,
                result_buffer,
                effect,
            )),
        }
    }

    pub(in crate::consumer::group) fn recover_terminal_after_driver_shutdown(
        &mut self,
        terminal: GroupPositionOffsetFetchTerminal,
        now: Moment,
    ) -> Result<(), ClassicGroupPositionRecoveryFault> {
        let fence = terminal.key().fence();
        match self.apply_raw_terminal(&terminal, now) {
            Ok(()) => {
                drop(terminal);
                self.recover_confirmation_after_driver_shutdown(fence)
            }
            Err(application) => Err(ClassicGroupPositionRecoveryFault::terminal(
                terminal,
                application,
            )),
        }
    }

    pub(in crate::consumer::group) fn recover_confirmation_after_driver_shutdown(
        &mut self,
        fence: kafka_client_core::GroupPositionFence,
    ) -> Result<(), ClassicGroupPositionRecoveryFault> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::ConfirmationPending(pending) = state else {
            self.set(state);
            return Err(ClassicGroupPositionRecoveryFault::fence(
                ClassicGroupPositionExecutionError::NotConfirmationPending,
                fence,
            ));
        };
        if pending.fence() != fence || pending.accepted().fence() != fence {
            let expected = pending.fence();
            self.set(ClassicGroupPositionExecutionState::ConfirmationPending(
                pending,
            ));
            return Err(ClassicGroupPositionRecoveryFault::fence(
                ClassicGroupPositionExecutionError::FenceMismatch {
                    expected,
                    supplied: fence,
                },
                fence,
            ));
        }
        let (completed, accepted) = pending.into_parts();
        drop(accepted);
        self.set(ClassicGroupPositionExecutionState::Complete(completed));
        Ok(())
    }
}
