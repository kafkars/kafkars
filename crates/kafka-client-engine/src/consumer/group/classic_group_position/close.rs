//! Exact local close of prepared or completed position bootstrap ownership.

use kafka_client_core::{GroupPositionBootstrapEffect, GroupPositionBootstrapInput, Moment};

use super::{
    ClassicGroupPositionCompleted, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    ClassicGroupPositionPrepared, ClassicGroupPositionRejectionFailure,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionCloseTurn {
    Idle,
    Progress,
    Blocked,
}

impl ClassicGroupPositionExecution {
    /// Settles an elapsed prepared request before bounded driver admission.
    #[expect(
        clippy::result_large_err,
        reason = "deadline failure retains the exact linear position owner without allocating"
    )]
    pub(in crate::consumer::group) fn expire_prepared_if_due(
        &mut self,
        now: Moment,
    ) -> Result<bool, ClassicGroupPositionRejectionFailure> {
        let due = matches!(
            self.state(),
            ClassicGroupPositionExecutionState::Prepared(prepared)
                if prepared.key().operation_deadline().core().is_elapsed_at(now)
        );
        if !due {
            return Ok(false);
        }
        self.close_position_if_local(now).map(|turn| {
            debug_assert_eq!(turn, ClassicGroupPositionCloseTurn::Progress);
            true
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "close failure retains the exact linear position owner without allocating"
    )]
    pub(in crate::consumer::group) fn close_position_if_local(
        &mut self,
        now: Moment,
    ) -> Result<ClassicGroupPositionCloseTurn, ClassicGroupPositionRejectionFailure> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let prepared = match state {
            ClassicGroupPositionExecutionState::Dormant => {
                return Ok(ClassicGroupPositionCloseTurn::Idle);
            }
            ClassicGroupPositionExecutionState::Complete(completed) => {
                let (machine, terminal, observed_at, operation_deadline) = completed.into_parts();
                drop((machine, terminal, observed_at, operation_deadline));
                return Ok(ClassicGroupPositionCloseTurn::Progress);
            }
            ClassicGroupPositionExecutionState::DriverOwned(owner) => {
                self.set(ClassicGroupPositionExecutionState::DriverOwned(owner));
                return Ok(ClassicGroupPositionCloseTurn::Blocked);
            }
            ClassicGroupPositionExecutionState::ConfirmationPending(pending) => {
                self.set(ClassicGroupPositionExecutionState::ConfirmationPending(
                    pending,
                ));
                return Ok(ClassicGroupPositionCloseTurn::Blocked);
            }
            ClassicGroupPositionExecutionState::ResetDriverOwned(owner) => {
                self.set(ClassicGroupPositionExecutionState::ResetDriverOwned(owner));
                return Ok(ClassicGroupPositionCloseTurn::Blocked);
            }
            ClassicGroupPositionExecutionState::ResetCompletionFault(fault) => {
                self.set(ClassicGroupPositionExecutionState::ResetCompletionFault(
                    fault,
                ));
                return Ok(ClassicGroupPositionCloseTurn::Blocked);
            }
            ClassicGroupPositionExecutionState::ResetTerminalFault(fault) => {
                self.set(ClassicGroupPositionExecutionState::ResetTerminalFault(
                    fault,
                ));
                return Ok(ClassicGroupPositionCloseTurn::Blocked);
            }
            ClassicGroupPositionExecutionState::ResetComplete(completed) => {
                drop(completed);
                return Ok(ClassicGroupPositionCloseTurn::Progress);
            }
            ClassicGroupPositionExecutionState::ResetPrepared(prepared) => {
                let fence = prepared.reset.fence();
                if let Err(error) = super::super::classic_group_position_reset::close_prepared_reset(
                    self, prepared, now,
                ) {
                    return Err(ClassicGroupPositionRejectionFailure::in_execution(
                        fence, error,
                    ));
                }
                drop(self.replace(ClassicGroupPositionExecutionState::Dormant));
                return Ok(ClassicGroupPositionCloseTurn::Progress);
            }
            ClassicGroupPositionExecutionState::Handoff(handoff) => {
                let fence = handoff.fence();
                self.set(ClassicGroupPositionExecutionState::Handoff(handoff));
                return Err(ClassicGroupPositionRejectionFailure::in_execution(
                    fence,
                    ClassicGroupPositionExecutionError::HandoffIncomplete,
                ));
            }
            ClassicGroupPositionExecutionState::Prepared(prepared) => prepared,
        };
        self.close_prepared(now, prepared)
    }

    #[expect(
        clippy::result_large_err,
        reason = "close failure retains the exact linear position owner without allocating"
    )]
    fn close_prepared(
        &mut self,
        now: Moment,
        prepared: ClassicGroupPositionPrepared,
    ) -> Result<ClassicGroupPositionCloseTurn, ClassicGroupPositionRejectionFailure> {
        let (key, mut machine, correlation, request, result_buffer) = prepared.into_parts();
        let fence = key.fence();
        let expected_deadline = key.operation_deadline().core();
        let transition =
            match machine.apply(GroupPositionBootstrapInput::DriverRejected { fence, now }) {
                Ok(transition) => transition,
                Err(error) => {
                    self.set(ClassicGroupPositionExecutionState::Prepared(
                        ClassicGroupPositionPrepared::new(
                            key,
                            machine,
                            correlation,
                            request,
                            result_buffer,
                        ),
                    ));
                    return Err(ClassicGroupPositionRejectionFailure::in_execution(
                        fence,
                        ClassicGroupPositionExecutionError::Core(error.kind()),
                    ));
                }
            };
        match transition.into_effect() {
            Some(GroupPositionBootstrapEffect::Complete {
                fence: effect_fence,
                deadline,
                terminal,
            }) => {
                self.set(ClassicGroupPositionExecutionState::Complete(
                    ClassicGroupPositionCompleted::new_with_operation_deadline(
                        machine,
                        terminal,
                        now,
                        key.operation_deadline(),
                    ),
                ));
                drop((correlation, request, result_buffer));
                if effect_fence != fence {
                    return Err(ClassicGroupPositionRejectionFailure::in_execution(
                        fence,
                        ClassicGroupPositionExecutionError::CompletionFence,
                    ));
                }
                if deadline != expected_deadline {
                    return Err(ClassicGroupPositionRejectionFailure::in_execution(
                        fence,
                        ClassicGroupPositionExecutionError::CompletionDeadline,
                    ));
                }
                drop(self.replace(ClassicGroupPositionExecutionState::Dormant));
                Ok(ClassicGroupPositionCloseTurn::Progress)
            }
            effect => {
                drop(request);
                Err(ClassicGroupPositionRejectionFailure::post_core(
                    fence,
                    ClassicGroupPositionExecutionError::DriverRejectedEffect,
                    machine,
                    correlation,
                    result_buffer,
                    effect,
                ))
            }
        }
    }
}
