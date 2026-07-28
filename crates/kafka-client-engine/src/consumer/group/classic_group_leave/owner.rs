//! Linear state owner for one explicit-close `LeaveGroup` attempt.

use std::sync::Arc;

use crate::{
    clock::OperationDeadline,
    driver::{
        ClassicGroupLeaveCall, ClassicGroupLeaveCompletionError, ClassicGroupLeaveRoute,
        classic_group::{
            ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationTerminalFailure,
        },
    },
};

use super::completion::{
    GroupConsumerCloseCompletion, GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
    GroupConsumerCloseTerminalFailureKind,
};

mod drive;
mod observation;

/// One bounded transition made by the per-entry owner.
#[derive(Debug)]
pub(in crate::consumer::group) enum ClassicGroupLeaveOwnerTurn {
    Idle,
    Progress,
    Blocked,
    Rediscover {
        route: ClassicGroupLeaveRoute,
        fallback: GroupConsumerCloseTerminal,
    },
}

impl PartialEq for ClassicGroupLeaveOwnerTurn {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Eq for ClassicGroupLeaveOwnerTurn {}

struct ClassicGroupLeaveFacts {
    group: Arc<str>,
    member: Arc<str>,
    group_instance_id: Option<Arc<str>>,
}

/// Per-entry broker leave ownership with at most one causally-authorized replacement.
pub(in crate::consumer::group) struct ClassicGroupLeaveOwner {
    completion: Option<Arc<GroupConsumerCloseCompletion>>,
    state: ClassicGroupLeaveState,
    replacement_used: bool,
    coordinator_invalidation_outstanding: bool,
}

enum ClassicGroupLeaveState {
    Dormant,
    Pending(OperationDeadline),
    RetryPending {
        deadline: OperationDeadline,
        facts: ClassicGroupLeaveFacts,
    },
    Prepared {
        deadline: OperationDeadline,
        facts: ClassicGroupLeaveFacts,
        request: crate::protocol::consumer::PreparedClassicLeaveGroupRequest,
    },
    DriverOwned {
        deadline: OperationDeadline,
        facts: ClassicGroupLeaveFacts,
        call: ClassicGroupLeaveCall,
    },
    RediscoveryTransfer {
        deadline: OperationDeadline,
        facts: ClassicGroupLeaveFacts,
    },
    AwaitingInvalidation {
        deadline: OperationDeadline,
        facts: ClassicGroupLeaveFacts,
    },
    CompletionFault {
        deadline: OperationDeadline,
        _call: ClassicGroupLeaveCall,
        _source: ClassicGroupLeaveCompletionError,
    },
    Terminal(GroupConsumerCloseTerminal),
}

impl ClassicGroupLeaveOwner {
    pub(in crate::consumer::group) const fn new() -> Self {
        Self {
            completion: None,
            state: ClassicGroupLeaveState::Dormant,
            replacement_used: false,
            coordinator_invalidation_outstanding: false,
        }
    }

    pub(in crate::consumer::group) fn begin(
        &mut self,
        deadline: OperationDeadline,
        completion: Arc<GroupConsumerCloseCompletion>,
    ) -> Result<(), Arc<GroupConsumerCloseCompletion>> {
        if !matches!(self.state, ClassicGroupLeaveState::Dormant) || self.completion.is_some() {
            return Err(completion);
        }
        self.completion = Some(completion);
        self.replacement_used = false;
        self.coordinator_invalidation_outstanding = false;
        self.state = ClassicGroupLeaveState::Pending(deadline);
        Ok(())
    }

    pub(in crate::consumer::group) fn confirm_rediscovery_transfer(&mut self) -> bool {
        let state = core::mem::replace(&mut self.state, ClassicGroupLeaveState::Dormant);
        let ClassicGroupLeaveState::RediscoveryTransfer { deadline, facts } = state else {
            self.state = state;
            return false;
        };
        self.replacement_used = true;
        self.coordinator_invalidation_outstanding = true;
        self.state = ClassicGroupLeaveState::AwaitingInvalidation { deadline, facts };
        true
    }

    pub(in crate::consumer::group) fn reject_rediscovery_transfer(
        &mut self,
        terminal: GroupConsumerCloseTerminal,
    ) -> bool {
        if !matches!(
            self.state,
            ClassicGroupLeaveState::RediscoveryTransfer { .. }
        ) {
            return false;
        }
        self.state = ClassicGroupLeaveState::Terminal(terminal);
        true
    }

    pub(in crate::consumer::group) fn complete_coordinator_invalidation(
        &mut self,
        result: Result<
            ClassicCoordinatorInvalidationPermission,
            ClassicCoordinatorInvalidationTerminalFailure,
        >,
    ) -> bool {
        if !self.coordinator_invalidation_outstanding {
            return false;
        }
        self.coordinator_invalidation_outstanding = false;
        let state = core::mem::replace(&mut self.state, ClassicGroupLeaveState::Dormant);
        match state {
            ClassicGroupLeaveState::AwaitingInvalidation { deadline, facts } => match result {
                Ok(
                    ClassicCoordinatorInvalidationPermission::Applied
                    | ClassicCoordinatorInvalidationPermission::IgnoredStale,
                ) => {
                    self.state = ClassicGroupLeaveState::RetryPending { deadline, facts };
                }
                Err(_failure) => self.fail(GroupConsumerCloseTerminalFailureKind::Transport),
            },
            state => self.state = state,
        }
        true
    }

    pub(in crate::consumer::group) fn resolve_no_member(&mut self) -> bool {
        let ClassicGroupLeaveState::Pending(_deadline) = self.state else {
            return false;
        };
        self.state = ClassicGroupLeaveState::Terminal(GroupConsumerCloseTerminal::Succeeded);
        true
    }

    pub(in crate::consumer::group) fn publish_terminal(&mut self) -> bool {
        let Some(completion) = self.completion.take() else {
            return matches!(self.state, ClassicGroupLeaveState::Dormant);
        };
        let ClassicGroupLeaveState::Terminal(terminal) = self.state else {
            self.completion = Some(completion);
            return false;
        };
        completion.publish(terminal)
    }

    pub(in crate::consumer::group) fn recover_after_driver_shutdown(&mut self) {
        match core::mem::replace(&mut self.state, ClassicGroupLeaveState::Dormant) {
            ClassicGroupLeaveState::Pending(_)
            | ClassicGroupLeaveState::RetryPending { .. }
            | ClassicGroupLeaveState::Prepared { .. }
            | ClassicGroupLeaveState::DriverOwned { .. }
            | ClassicGroupLeaveState::RediscoveryTransfer { .. }
            | ClassicGroupLeaveState::AwaitingInvalidation { .. }
            | ClassicGroupLeaveState::CompletionFault { .. } => {
                self.fail(GroupConsumerCloseTerminalFailureKind::DriverShutdown);
            }
            state => self.state = state,
        }
    }

    fn fail(&mut self, kind: GroupConsumerCloseTerminalFailureKind) {
        let failure = GroupConsumerCloseTerminalFailure {
            kind,
            broker_code: None,
        };
        self.state = ClassicGroupLeaveState::Terminal(GroupConsumerCloseTerminal::Failed(failure));
    }
}
