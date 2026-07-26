//! Lossless position request handoff and core admission transitions.

use kafka_client_core::{
    GroupPositionBootstrapEffect, GroupPositionBootstrapMachineError, GroupPositionFence,
    GroupPositionPartitionFact,
};

use crate::{
    driver::{GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchKey},
    protocol::consumer::PreparedGroupOffsetFetchRequest,
};

use super::{
    ClassicGroupPositionExecution, ClassicGroupPositionExecutionState, ClassicGroupPositionHandoff,
    ClassicGroupPositionPrepared,
};

/// Exact local state or core disagreement during one synchronous handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupPositionExecutionError {
    NotPrepared,
    NotInHandoff,
    HandoffIncomplete,
    NotDriverOwned,
    NotConfirmationPending,
    FenceMismatch {
        expected: GroupPositionFence,
        supplied: GroupPositionFence,
    },
    DeadlineMismatch,
    ResultBuffer,
    TerminalCorrelation,
    Confirmation,
    Core(GroupPositionBootstrapMachineError),
    DriverAcceptedEffect,
    DriverRejectedEffect,
    TerminalEffect,
    CompletionFence,
    CompletionDeadline,
}

enum ClassicGroupPositionAcceptanceFailureOwnership {
    Receipt(GroupPositionOffsetFetchAccepted),
    Execution,
}

/// Failed accepted-receipt integration retaining its exact current owner.
#[must_use = "a failed position acceptance still owns or locates its driver receipt"]
pub(in crate::consumer::group) struct ClassicGroupPositionAcceptanceFailure {
    fence: GroupPositionFence,
    error: ClassicGroupPositionExecutionError,
    ownership: ClassicGroupPositionAcceptanceFailureOwnership,
}

impl ClassicGroupPositionAcceptanceFailure {
    pub(super) const fn pre_core(
        accepted: GroupPositionOffsetFetchAccepted,
        error: ClassicGroupPositionExecutionError,
    ) -> Self {
        Self {
            fence: accepted.fence(),
            error,
            ownership: ClassicGroupPositionAcceptanceFailureOwnership::Receipt(accepted),
        }
    }

    pub(super) const fn post_core(
        fence: GroupPositionFence,
        error: ClassicGroupPositionExecutionError,
    ) -> Self {
        Self {
            fence,
            error,
            ownership: ClassicGroupPositionAcceptanceFailureOwnership::Execution,
        }
    }

    pub(in crate::consumer::group) const fn error(&self) -> ClassicGroupPositionExecutionError {
        self.error
    }

    pub(in crate::consumer::group) fn retained_owner_count(&self) -> usize {
        let _ = (self.fence, self.error);
        if let ClassicGroupPositionAcceptanceFailureOwnership::Receipt(accepted) = &self.ownership {
            let _ = accepted.fence();
        }
        1
    }
}

/// Mutated core and effect retained outside ordinary execution after an impossible mismatch.
#[must_use = "post-core rejection ownership must remain frozen until recovery"]
struct ClassicGroupPositionRejectionPostCore {
    _machine: kafka_client_core::GroupPositionBootstrapMachine,
    _correlation: crate::protocol::consumer::GroupOffsetFetchCorrelation,
    _result_buffer: Vec<GroupPositionPartitionFact>,
    _effect: Option<GroupPositionBootstrapEffect>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "post-core failure must retain every exact owner without allocating on the fault path"
)]
enum ClassicGroupPositionRejectionFailureOwnership {
    Execution,
    PostCore {
        _owner: ClassicGroupPositionRejectionPostCore,
    },
}

/// Exact pre-core or post-core failure applying one local driver rejection.
#[must_use = "a failed driver rejection retains its exact current core owner"]
pub(in crate::consumer::group) struct ClassicGroupPositionRejectionFailure {
    fence: GroupPositionFence,
    error: ClassicGroupPositionExecutionError,
    ownership: ClassicGroupPositionRejectionFailureOwnership,
}

impl ClassicGroupPositionRejectionFailure {
    pub(super) const fn in_execution(
        fence: GroupPositionFence,
        error: ClassicGroupPositionExecutionError,
    ) -> Self {
        Self {
            fence,
            error,
            ownership: ClassicGroupPositionRejectionFailureOwnership::Execution,
        }
    }

    pub(super) fn post_core(
        fence: GroupPositionFence,
        error: ClassicGroupPositionExecutionError,
        machine: kafka_client_core::GroupPositionBootstrapMachine,
        correlation: crate::protocol::consumer::GroupOffsetFetchCorrelation,
        result_buffer: Vec<GroupPositionPartitionFact>,
        effect: Option<GroupPositionBootstrapEffect>,
    ) -> Self {
        Self {
            fence,
            error,
            ownership: ClassicGroupPositionRejectionFailureOwnership::PostCore {
                _owner: ClassicGroupPositionRejectionPostCore {
                    _machine: machine,
                    _correlation: correlation,
                    _result_buffer: result_buffer,
                    _effect: effect,
                },
            },
        }
    }

    pub(in crate::consumer::group) const fn error(&self) -> ClassicGroupPositionExecutionError {
        self.error
    }

    pub(in crate::consumer::group) fn retained_owner_count(&self) -> usize {
        let _ = (self.fence, self.error, &self.ownership);
        1
    }
}

impl ClassicGroupPositionExecution {
    pub(in crate::consumer::group) const fn is_prepared(&self) -> bool {
        matches!(
            self.state(),
            ClassicGroupPositionExecutionState::Prepared(_)
        )
    }

    pub(in crate::consumer::group) fn begin_handoff(
        &mut self,
    ) -> Result<
        (GroupPositionOffsetFetchKey, PreparedGroupOffsetFetchRequest),
        ClassicGroupPositionExecutionError,
    > {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::Prepared(prepared) = state else {
            self.set(state);
            return Err(ClassicGroupPositionExecutionError::NotPrepared);
        };
        let (key, machine, correlation, request, result_buffer) = prepared.into_parts();
        self.set(ClassicGroupPositionExecutionState::Handoff(
            ClassicGroupPositionHandoff::new(machine, correlation, result_buffer),
        ));
        Ok((key, request))
    }

    pub(in crate::consumer::group) fn handoff_group(
        &self,
    ) -> Result<&str, ClassicGroupPositionExecutionError> {
        match self.state() {
            ClassicGroupPositionExecutionState::Handoff(handoff) => {
                Ok(handoff.correlation().group_id())
            }
            _ => Err(ClassicGroupPositionExecutionError::NotInHandoff),
        }
    }

    pub(in crate::consumer::group) fn restore_prepared(
        &mut self,
        key: GroupPositionOffsetFetchKey,
        request: PreparedGroupOffsetFetchRequest,
    ) -> Result<(), ClassicGroupPositionExecutionError> {
        let state = self.replace(ClassicGroupPositionExecutionState::Dormant);
        let ClassicGroupPositionExecutionState::Handoff(handoff) = state else {
            self.set(state);
            return Err(ClassicGroupPositionExecutionError::NotInHandoff);
        };
        let expected_fence = handoff.fence();
        let expected_deadline = handoff.deadline();
        let supplied_fence = key.fence();
        let supplied_deadline = key.operation_deadline().core();
        let (machine, correlation, result_buffer) = handoff.into_parts();
        self.set(ClassicGroupPositionExecutionState::Prepared(
            ClassicGroupPositionPrepared::new(key, machine, correlation, request, result_buffer),
        ));
        if expected_fence != supplied_fence {
            return Err(ClassicGroupPositionExecutionError::FenceMismatch {
                expected: expected_fence,
                supplied: supplied_fence,
            });
        }
        if expected_deadline != supplied_deadline {
            return Err(ClassicGroupPositionExecutionError::DeadlineMismatch);
        }
        Ok(())
    }
}
