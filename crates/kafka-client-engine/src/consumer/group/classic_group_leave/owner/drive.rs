//! Deadline-bounded preparation, submission, and terminal settlement for classic leave.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::{
    clock::OperationDeadline,
    driver::{ClassicGroupLeaveCall, DriverOwner},
    protocol::consumer::classic_leave_group_request_with_instance,
};

use super::{
    ClassicGroupLeaveFacts, ClassicGroupLeaveOwner, ClassicGroupLeaveOwnerTurn,
    ClassicGroupLeaveState,
};
use crate::consumer::group::classic_group_leave::{
    completion::{GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailureKind},
    terminal::{normalize_terminal, should_rediscover},
};

impl ClassicGroupLeaveOwner {
    pub(in crate::consumer::group) fn turn_owned_with_instance(
        &mut self,
        now: Moment,
        group: Arc<str>,
        member: Option<Arc<str>>,
        group_instance_id: Option<Arc<str>>,
        membership_call_pending: bool,
        driver: &DriverOwner,
    ) -> ClassicGroupLeaveOwnerTurn {
        match &self.state {
            ClassicGroupLeaveState::Dormant | ClassicGroupLeaveState::Terminal(_) => {
                return ClassicGroupLeaveOwnerTurn::Idle;
            }
            ClassicGroupLeaveState::CompletionFault { .. }
            | ClassicGroupLeaveState::RediscoveryTransfer { .. } => {
                return ClassicGroupLeaveOwnerTurn::Blocked;
            }
            ClassicGroupLeaveState::AwaitingInvalidation { deadline, .. } => {
                if deadline.core().is_elapsed_at(now) {
                    self.fail(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed);
                    return ClassicGroupLeaveOwnerTurn::Progress;
                }
                return ClassicGroupLeaveOwnerTurn::Blocked;
            }
            ClassicGroupLeaveState::Pending(deadline) => {
                let deadline = *deadline;
                if let Some(member) = member {
                    if deadline.core().is_elapsed_at(now) {
                        self.fail(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed);
                        return ClassicGroupLeaveOwnerTurn::Progress;
                    }
                    let facts = ClassicGroupLeaveFacts {
                        group,
                        member,
                        group_instance_id,
                    };
                    self.prepare(deadline, facts);
                } else if membership_call_pending {
                    if deadline.core().is_elapsed_at(now) {
                        self.fail(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed);
                        return ClassicGroupLeaveOwnerTurn::Progress;
                    }
                    return ClassicGroupLeaveOwnerTurn::Blocked;
                } else {
                    self.state =
                        ClassicGroupLeaveState::Terminal(GroupConsumerCloseTerminal::Succeeded);
                }
                return ClassicGroupLeaveOwnerTurn::Progress;
            }
            ClassicGroupLeaveState::RetryPending { .. } => {
                let state = core::mem::replace(&mut self.state, ClassicGroupLeaveState::Dormant);
                let ClassicGroupLeaveState::RetryPending { deadline, facts } = state else {
                    unreachable!("matched retry-pending state")
                };
                if deadline.core().is_elapsed_at(now) {
                    self.fail(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed);
                } else {
                    self.prepare(deadline, facts);
                }
                return ClassicGroupLeaveOwnerTurn::Progress;
            }
            ClassicGroupLeaveState::Prepared { deadline, .. } => {
                if deadline.core().is_elapsed_at(now) {
                    self.fail(GroupConsumerCloseTerminalFailureKind::DeadlineElapsed);
                    return ClassicGroupLeaveOwnerTurn::Progress;
                }
            }
            ClassicGroupLeaveState::DriverOwned { .. } => {}
        }
        if matches!(self.state, ClassicGroupLeaveState::Prepared { .. }) {
            return self.submit(driver);
        }
        self.settle(now)
    }

    fn prepare(&mut self, deadline: OperationDeadline, facts: ClassicGroupLeaveFacts) {
        match classic_leave_group_request_with_instance(
            &facts.group,
            &facts.member,
            facts.group_instance_id.as_deref(),
        ) {
            Ok(request) => {
                self.state = ClassicGroupLeaveState::Prepared {
                    deadline,
                    facts,
                    request,
                };
            }
            Err(_error) => self.fail(GroupConsumerCloseTerminalFailureKind::InvalidResponse),
        }
    }

    fn submit(&mut self, driver: &DriverOwner) -> ClassicGroupLeaveOwnerTurn {
        let state = core::mem::replace(&mut self.state, ClassicGroupLeaveState::Dormant);
        let ClassicGroupLeaveState::Prepared {
            deadline,
            facts,
            request,
        } = state
        else {
            self.state = state;
            return ClassicGroupLeaveOwnerTurn::Idle;
        };
        match ClassicGroupLeaveCall::submit(driver, &facts.group, request, deadline) {
            Ok(call) => {
                self.state = ClassicGroupLeaveState::DriverOwned {
                    deadline,
                    facts,
                    call,
                };
            }
            Err(_error) => self.fail(GroupConsumerCloseTerminalFailureKind::DriverRejected),
        }
        ClassicGroupLeaveOwnerTurn::Progress
    }

    fn settle(&mut self, now: Moment) -> ClassicGroupLeaveOwnerTurn {
        let ClassicGroupLeaveState::DriverOwned { deadline, call, .. } = &self.state else {
            return ClassicGroupLeaveOwnerTurn::Idle;
        };
        let Some(result) = call.try_result() else {
            return ClassicGroupLeaveOwnerTurn::Blocked;
        };
        let deadline = *deadline;
        let state = core::mem::replace(&mut self.state, ClassicGroupLeaveState::Dormant);
        let ClassicGroupLeaveState::DriverOwned { facts, call, .. } = state else {
            self.state = state;
            return ClassicGroupLeaveOwnerTurn::Idle;
        };
        match result {
            Err(source) => {
                self.state = ClassicGroupLeaveState::CompletionFault {
                    deadline,
                    _call: call,
                    _source: source,
                };
                ClassicGroupLeaveOwnerTurn::Progress
            }
            Ok(outcome) => {
                drop(call);
                let (resolution, route) = outcome.into_resolution();
                let fallback = normalize_terminal(deadline, now, resolution);
                let retryable = should_rediscover(
                    deadline.core().is_elapsed_at(now),
                    self.replacement_used,
                    resolution,
                );
                if retryable {
                    self.state = ClassicGroupLeaveState::RediscoveryTransfer { deadline, facts };
                    ClassicGroupLeaveOwnerTurn::Rediscover { route, fallback }
                } else {
                    route.accept();
                    self.state = ClassicGroupLeaveState::Terminal(fallback);
                    ClassicGroupLeaveOwnerTurn::Progress
                }
            }
        }
    }
}
