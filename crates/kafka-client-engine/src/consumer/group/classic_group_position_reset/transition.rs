//! Deterministic owner transfer into, through, and out of reset execution.

use kafka_client_core::{
    GroupPositionBootstrapTerminal, GroupPositionFence, GroupPositionResetEffect,
    GroupPositionResetInput, GroupPositionResetMachine, GroupPositionResetTerminal, Moment,
    PositionResolutionAttemptFailure,
};

use super::{
    super::classic_group_position::{
        ClassicGroupPositionCompleted, ClassicGroupPositionExecution,
        ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    },
    state::{
        ClassicGroupPositionResetCompleted, ClassicGroupPositionResetDriverOwned,
        ClassicGroupPositionResetPrepared,
    },
};
use crate::clock::OperationDeadline;

impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) fn begin_missing_offset_reset(
        &mut self,
        current: GroupPositionFence,
        now: Moment,
    ) -> Result<(), ClassicGroupPositionExecutionError> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::Complete(completed) = state else {
            self.set(state);
            return Err(ClassicGroupPositionExecutionError::ResetNotRequired);
        };
        if completed.fence() != current {
            self.set(ClassicGroupPositionExecutionState::Complete(completed));
            return Err(ClassicGroupPositionExecutionError::ResetCurrentFence);
        }
        let (bootstrap, terminal, offset_fetch_observed_at, operation_deadline) =
            completed.into_parts();
        let GroupPositionBootstrapTerminal::ResetRequired(required) = terminal else {
            self.set(ClassicGroupPositionExecutionState::Complete(
                ClassicGroupPositionCompleted::new_with_operation_deadline(
                    bootstrap,
                    terminal,
                    offset_fetch_observed_at,
                    operation_deadline,
                ),
            ));
            return Err(ClassicGroupPositionExecutionError::ResetNotRequired);
        };
        let mut reset =
            GroupPositionResetMachine::new(current, operation_deadline.core(), required);
        let transition = reset
            .apply(GroupPositionResetInput::Start {
                fence: current,
                now,
            })
            .map_err(|error| ClassicGroupPositionExecutionError::ResetCore(error.kind()))?;
        install_reset_transition(self, bootstrap, reset, operation_deadline, now, transition)
    }

    pub(in crate::consumer::group) fn recover_reset_after_driver_shutdown(&mut self) {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        match state {
            ClassicGroupPositionExecutionState::ResetDriverOwned(owner) => {
                recover_driver_owned(self, owner);
            }
            ClassicGroupPositionExecutionState::ResetCompletionFault(fault) => drop(fault),
            ClassicGroupPositionExecutionState::ResetTerminalFault(fault) => drop(fault),
            state => self.set(state),
        }
    }
}

fn recover_driver_owned(
    execution: &mut ClassicGroupPositionExecution,
    owner: ClassicGroupPositionResetDriverOwned,
) {
    let ClassicGroupPositionResetDriverOwned {
        bootstrap,
        mut reset,
        operation_deadline,
        partition,
        topic: _,
        isolation: _,
        call,
    } = owner;
    drop(call);
    let now = Moment::from_tick(u64::MAX);
    let transition = reset.apply(GroupPositionResetInput::ResolutionFailed {
        fence: reset.fence(),
        partition,
        now,
        failure: PositionResolutionAttemptFailure::Transport,
    });
    if let Ok(transition) = transition {
        let _result = install_reset_transition(
            execution,
            bootstrap,
            reset,
            operation_deadline,
            now,
            transition,
        );
    }
}

pub(in crate::consumer::group) fn install_reset_transition(
    execution: &mut ClassicGroupPositionExecution,
    bootstrap: kafka_client_core::GroupPositionBootstrapMachine,
    reset: GroupPositionResetMachine,
    operation_deadline: OperationDeadline,
    observed_at: Moment,
    transition: kafka_client_core::GroupPositionResetTransition,
) -> Result<(), ClassicGroupPositionExecutionError> {
    match transition.into_effect() {
        Some(GroupPositionResetEffect::ResolveOffset {
            fence,
            deadline,
            partition,
            position,
        }) => {
            validate_effect(&reset, operation_deadline, fence, deadline)?;
            execution.set(ClassicGroupPositionExecutionState::ResetPrepared(
                ClassicGroupPositionResetPrepared {
                    bootstrap,
                    reset,
                    operation_deadline,
                    partition,
                    position,
                },
            ));
            Ok(())
        }
        Some(GroupPositionResetEffect::Complete {
            fence,
            deadline,
            terminal,
        }) => {
            validate_effect(&reset, operation_deadline, fence, deadline)?;
            install_terminal(
                execution,
                bootstrap,
                reset,
                operation_deadline,
                observed_at,
                terminal,
            );
            Ok(())
        }
        None => Err(ClassicGroupPositionExecutionError::ResetEffect),
    }
}

fn validate_effect(
    reset: &GroupPositionResetMachine,
    operation_deadline: OperationDeadline,
    fence: GroupPositionFence,
    deadline: kafka_client_core::Deadline,
) -> Result<(), ClassicGroupPositionExecutionError> {
    if fence != reset.fence() {
        return Err(ClassicGroupPositionExecutionError::ResetFence);
    }
    if deadline != operation_deadline.core() {
        return Err(ClassicGroupPositionExecutionError::ResetDeadline);
    }
    Ok(())
}

fn install_terminal(
    execution: &mut ClassicGroupPositionExecution,
    bootstrap: kafka_client_core::GroupPositionBootstrapMachine,
    reset: GroupPositionResetMachine,
    operation_deadline: OperationDeadline,
    observed_at: Moment,
    terminal: GroupPositionResetTerminal,
) {
    match terminal {
        GroupPositionResetTerminal::Ready(batch) => {
            execution.set(ClassicGroupPositionExecutionState::Complete(
                ClassicGroupPositionCompleted::new_with_operation_deadline(
                    bootstrap,
                    GroupPositionBootstrapTerminal::Ready(batch),
                    observed_at,
                    operation_deadline,
                ),
            ));
        }
        terminal @ GroupPositionResetTerminal::Failed(_) => execution.set(
            ClassicGroupPositionExecutionState::ResetComplete(ClassicGroupPositionResetCompleted {
                _bootstrap: bootstrap,
                _reset: reset,
                terminal,
                _operation_deadline: operation_deadline,
                _observed_at: observed_at,
            }),
        ),
    }
}

pub(in crate::consumer::group) fn close_prepared_reset(
    execution: &mut ClassicGroupPositionExecution,
    prepared: ClassicGroupPositionResetPrepared,
    now: Moment,
) -> Result<(), ClassicGroupPositionExecutionError> {
    super::submission::settle_local_rejection(execution, prepared, now)
}
