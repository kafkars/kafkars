//! Lossless core application and confirmation staging for one raw terminal.

use kafka_client_core::{
    GroupPositionBootstrapEffect, GroupPositionBootstrapInput, GroupPositionPartitionFact, Moment,
};

use crate::{
    driver::{GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchTerminal},
    protocol::consumer::GroupOffsetFetchCorrelation,
};

use super::{
    ClassicGroupPositionCompleted, ClassicGroupPositionConfirmationPending,
    ClassicGroupPositionDriverOwned, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    terminal_normalization::normalize_terminal,
};

#[expect(
    clippy::large_enum_variant,
    reason = "post-core failure retains exact owners without allocating on the fault path"
)]
enum ClassicGroupPositionTerminalApplicationOwnership {
    RestoredExecution,
    AppliedExecution,
    PostCore {
        _owner: ClassicGroupPositionTerminalPostCore,
    },
}

/// Mutated core facts retained after an impossible nonterminal effect.
#[must_use = "post-core position ownership must remain frozen until recovery"]
struct ClassicGroupPositionTerminalPostCore {
    _machine: kafka_client_core::GroupPositionBootstrapMachine,
    _correlation: GroupOffsetFetchCorrelation,
    _accepted: GroupPositionOffsetFetchAccepted,
    _spare_buffer: Option<Vec<GroupPositionPartitionFact>>,
    _effect: Option<GroupPositionBootstrapEffect>,
}

/// Failed terminal application locating every exact linear owner.
#[must_use = "failed terminal application must restore or freeze its raw terminal"]
pub(in crate::consumer::group) struct ClassicGroupPositionTerminalApplicationFailure {
    error: ClassicGroupPositionExecutionError,
    ownership: ClassicGroupPositionTerminalApplicationOwnership,
}

impl ClassicGroupPositionTerminalApplicationFailure {
    const fn restored(error: ClassicGroupPositionExecutionError) -> Self {
        Self {
            error,
            ownership: ClassicGroupPositionTerminalApplicationOwnership::RestoredExecution,
        }
    }

    const fn applied(error: ClassicGroupPositionExecutionError) -> Self {
        Self {
            error,
            ownership: ClassicGroupPositionTerminalApplicationOwnership::AppliedExecution,
        }
    }

    fn post_core(
        error: ClassicGroupPositionExecutionError,
        owner: ClassicGroupPositionTerminalPostCore,
    ) -> Self {
        Self {
            error,
            ownership: ClassicGroupPositionTerminalApplicationOwnership::PostCore { _owner: owner },
        }
    }

    pub(in crate::consumer::group) const fn error(&self) -> ClassicGroupPositionExecutionError {
        self.error
    }

    pub(in crate::consumer::group) const fn raw_terminal_is_restorable(&self) -> bool {
        matches!(
            &self.ownership,
            ClassicGroupPositionTerminalApplicationOwnership::RestoredExecution
        )
    }
}

#[expect(
    clippy::result_large_err,
    reason = "terminal failure retains exact core, receipt, and result owners without allocation"
)]
impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) fn apply_raw_terminal(
        &mut self,
        terminal: &GroupPositionOffsetFetchTerminal,
        now: Moment,
    ) -> Result<(), ClassicGroupPositionTerminalApplicationFailure> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::DriverOwned(owner) = state else {
            self.set(state);
            return Err(ClassicGroupPositionTerminalApplicationFailure::restored(
                ClassicGroupPositionExecutionError::NotDriverOwned,
            ));
        };
        let (mut machine, correlation, accepted, result_buffer) = owner.into_parts();
        let expected = machine.fence();
        let expected_deadline = machine.deadline();
        let supplied = terminal.key().fence();
        if expected != supplied || accepted.fence() != supplied {
            self.restore_driver_owned(machine, correlation, accepted, result_buffer);
            return Err(ClassicGroupPositionTerminalApplicationFailure::restored(
                ClassicGroupPositionExecutionError::FenceMismatch { expected, supplied },
            ));
        }
        if expected_deadline != terminal.key().operation_deadline().core() {
            self.restore_driver_owned(machine, correlation, accepted, result_buffer);
            return Err(ClassicGroupPositionTerminalApplicationFailure::restored(
                ClassicGroupPositionExecutionError::DeadlineMismatch,
            ));
        }
        let normalized =
            match normalize_terminal(&machine, &correlation, terminal, now, result_buffer) {
                Ok(normalized) => normalized,
                Err((error, result_buffer)) => {
                    self.restore_driver_owned(machine, correlation, accepted, result_buffer);
                    return Err(ClassicGroupPositionTerminalApplicationFailure::restored(
                        error,
                    ));
                }
            };
        let (input, spare_buffer) = normalized.into_parts();
        let transition = match machine.apply(input) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = error.kind();
                let result_buffer = recover_result_buffer(error.into_input(), spare_buffer);
                self.restore_driver_owned(machine, correlation, accepted, result_buffer);
                return Err(ClassicGroupPositionTerminalApplicationFailure::restored(
                    ClassicGroupPositionExecutionError::Core(kind),
                ));
            }
        };
        stage_terminal_effect(
            self,
            supplied,
            expected_deadline,
            now,
            machine,
            correlation,
            accepted,
            spare_buffer,
            transition.into_effect(),
        )
    }

    fn restore_driver_owned(
        &mut self,
        machine: kafka_client_core::GroupPositionBootstrapMachine,
        correlation: GroupOffsetFetchCorrelation,
        accepted: GroupPositionOffsetFetchAccepted,
        result_buffer: Vec<GroupPositionPartitionFact>,
    ) {
        self.set(ClassicGroupPositionExecutionState::DriverOwned(
            ClassicGroupPositionDriverOwned::new(machine, correlation, accepted, result_buffer),
        ));
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the exact core, protocol, receipt, buffer, and effect owners cross one atomic seam"
)]
#[expect(
    clippy::result_large_err,
    reason = "post-core failure retains every exact owner without allocating"
)]
pub(super) fn stage_terminal_effect(
    execution: &mut ClassicGroupPositionExecution,
    supplied: kafka_client_core::GroupPositionFence,
    expected_deadline: kafka_client_core::Deadline,
    observed_at: Moment,
    machine: kafka_client_core::GroupPositionBootstrapMachine,
    correlation: GroupOffsetFetchCorrelation,
    accepted: GroupPositionOffsetFetchAccepted,
    spare_buffer: Option<Vec<GroupPositionPartitionFact>>,
    effect: Option<GroupPositionBootstrapEffect>,
) -> Result<(), ClassicGroupPositionTerminalApplicationFailure> {
    match effect {
        Some(GroupPositionBootstrapEffect::Complete {
            fence,
            deadline,
            terminal,
        }) => {
            execution.set(ClassicGroupPositionExecutionState::ConfirmationPending(
                ClassicGroupPositionConfirmationPending::new(
                    ClassicGroupPositionCompleted::new(machine, terminal, observed_at),
                    accepted,
                ),
            ));
            if fence != supplied {
                return Err(ClassicGroupPositionTerminalApplicationFailure::applied(
                    ClassicGroupPositionExecutionError::CompletionFence,
                ));
            }
            if deadline != expected_deadline {
                return Err(ClassicGroupPositionTerminalApplicationFailure::applied(
                    ClassicGroupPositionExecutionError::CompletionDeadline,
                ));
            }
            Ok(())
        }
        effect => Err(ClassicGroupPositionTerminalApplicationFailure::post_core(
            ClassicGroupPositionExecutionError::TerminalEffect,
            ClassicGroupPositionTerminalPostCore {
                _machine: machine,
                _correlation: correlation,
                _accepted: accepted,
                _spare_buffer: spare_buffer,
                _effect: effect,
            },
        )),
    }
}

fn recover_result_buffer(
    input: GroupPositionBootstrapInput,
    spare_buffer: Option<Vec<GroupPositionPartitionFact>>,
) -> Vec<GroupPositionPartitionFact> {
    match input {
        GroupPositionBootstrapInput::OffsetsFetched { batch, .. } => batch.into_parts().1,
        _ => spare_buffer.unwrap_or_default(),
    }
}
