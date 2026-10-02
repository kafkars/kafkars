//! Leave ownership observations and exactly-once terminal publication.

use super::{ClassicGroupLeaveOwner, ClassicGroupLeaveState};
use crate::consumer::group::classic_group_leave::completion::{
    GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailureKind,
};

impl ClassicGroupLeaveOwner {
    pub(in crate::consumer::group) fn expire_unpublished_success(
        &mut self,
        now: kafka_client_core::Moment,
    ) {
        if self.completion.is_some()
            && matches!(
                self.state,
                ClassicGroupLeaveState::Terminal(GroupConsumerCloseTerminal::Succeeded)
            )
            && self
                .completion_deadline
                .is_some_and(|deadline| deadline.core().is_elapsed_at(now))
        {
            self.fail(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed);
        }
    }

    pub(in crate::consumer::group) fn publish_failed_terminal(&mut self) -> bool {
        if !self.failed_terminal_is_unpublished() || !self.publish_terminal() {
            return false;
        }
        self.early_failure_published = true;
        true
    }

    pub(in crate::consumer::group) fn failed_terminal_is_unpublished(&self) -> bool {
        self.completion.is_some()
            && matches!(
                self.state,
                ClassicGroupLeaveState::Terminal(GroupConsumerCloseTerminal::Failed(_))
            )
    }

    pub(in crate::consumer::group) fn publish_terminal(&mut self) -> bool {
        let Some(completion) = self.completion.take() else {
            return self.early_failure_published
                || matches!(self.state, ClassicGroupLeaveState::Dormant);
        };
        let ClassicGroupLeaveState::Terminal(terminal) = self.state else {
            self.completion = Some(completion);
            return false;
        };
        if !completion.publish(terminal) {
            self.completion = Some(completion);
            return false;
        }
        self.completion_deadline = None;
        true
    }

    pub(in crate::consumer::group) const fn owns_coordinator_invalidation(&self) -> bool {
        self.coordinator_invalidation_outstanding
    }

    pub(in crate::consumer::group) fn clear_coordinator_invalidation_after_driver_shutdown(
        &mut self,
    ) {
        self.coordinator_invalidation_outstanding = false;
    }

    pub(in crate::consumer::group) fn allows_local_close(&self) -> bool {
        !self.coordinator_invalidation_outstanding
            && matches!(
                self.state,
                ClassicGroupLeaveState::Dormant | ClassicGroupLeaveState::Terminal(_)
            )
    }

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        match &self.state {
            ClassicGroupLeaveState::Pending(deadline)
            | ClassicGroupLeaveState::RetryPending { deadline, .. }
            | ClassicGroupLeaveState::Prepared { deadline, .. }
            | ClassicGroupLeaveState::DriverOwned { deadline, .. }
            | ClassicGroupLeaveState::RediscoveryTransfer { deadline, .. }
            | ClassicGroupLeaveState::AwaitingInvalidation { deadline, .. }
            | ClassicGroupLeaveState::CompletionFault { deadline, .. } => Some(deadline.core()),
            ClassicGroupLeaveState::Terminal(_) => {
                self.completion_deadline.map(super::OperationDeadline::core)
            }
            ClassicGroupLeaveState::Dormant => None,
        }
    }

    pub(in crate::consumer::group) fn unsettled(&self) -> usize {
        usize::from(!self.allows_local_close())
    }
}
