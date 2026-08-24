//! Two-phase group commit input handoff and exact route-token confirmation.

use std::mem;

use kafka_client_core::{GroupOffsetCommitInput, OperationId};
use kafka_driver::{Call, InvalidationDisposition, RouteFailureToken};

use crate::driver::DriverOwner;

use super::group_offset_commit_retry::{
    GroupOffsetCommitRefreshPoll, GroupOffsetCommitRetryCandidate, RouteTokenDestination,
    route_token_destination,
};

pub(super) struct SettledGroupOffsetCommitCall {
    operation_id: OperationId,
    fact: GroupOffsetCommitSettlementFact,
    route_token: Option<RouteFailureToken>,
    coordinator_refresh: CoordinatorRefresh,
}

impl SettledGroupOffsetCommitCall {
    pub(super) fn new(
        operation_id: OperationId,
        input: GroupOffsetCommitInput,
        mut route_token: Option<RouteFailureToken>,
    ) -> Self {
        let coordinator_refresh = match route_token_destination(&input) {
            RouteTokenDestination::Confirm => CoordinatorRefresh::None,
            RouteTokenDestination::Refresh => route_token
                .take()
                .map_or(CoordinatorRefresh::None, CoordinatorRefresh::Queued),
        };
        Self {
            operation_id,
            fact: GroupOffsetCommitSettlementFact::Terminal(input),
            route_token,
            coordinator_refresh,
        }
    }

    pub(super) fn new_retry(
        candidate: GroupOffsetCommitRetryCandidate,
        route_token: RouteFailureToken,
    ) -> Self {
        Self {
            operation_id: candidate.operation_id(),
            fact: GroupOffsetCommitSettlementFact::Retry(candidate),
            route_token: None,
            coordinator_refresh: CoordinatorRefresh::Queued(route_token),
        }
    }

    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(super) fn poll_coordinator_refresh(
        &mut self,
        driver: &DriverOwner,
    ) -> GroupOffsetCommitRefreshPoll {
        match mem::replace(&mut self.coordinator_refresh, CoordinatorRefresh::None) {
            CoordinatorRefresh::None => self.ready_poll(),
            CoordinatorRefresh::Queued(route_token) => {
                match driver.driver.invalidate(route_token) {
                    Ok(call) => {
                        self.coordinator_refresh = CoordinatorRefresh::Active(call);
                        return GroupOffsetCommitRefreshPoll::Submitted;
                    }
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        self.coordinator_refresh = CoordinatorRefresh::Queued(route_token);
                    }
                }
                GroupOffsetCommitRefreshPoll::Pending
            }
            CoordinatorRefresh::Active(call) => {
                if call.try_result().is_none() {
                    self.coordinator_refresh = CoordinatorRefresh::Active(call);
                    GroupOffsetCommitRefreshPoll::Pending
                } else {
                    self.ready_poll()
                }
            }
        }
    }

    fn ready_poll(&self) -> GroupOffsetCommitRefreshPoll {
        if matches!(self.fact, GroupOffsetCommitSettlementFact::Retry(_)) {
            GroupOffsetCommitRefreshPoll::ReplacementReady
        } else {
            GroupOffsetCommitRefreshPoll::Ready
        }
    }

    pub(super) fn expire_coordinator_refresh(&mut self) {
        drop(mem::replace(
            &mut self.coordinator_refresh,
            CoordinatorRefresh::None,
        ));
        let fact = mem::replace(
            &mut self.fact,
            GroupOffsetCommitSettlementFact::Terminal(GroupOffsetCommitInput::InvalidResponse),
        );
        self.fact = match fact {
            GroupOffsetCommitSettlementFact::Retry(candidate) => {
                GroupOffsetCommitSettlementFact::Terminal(candidate.into_terminal())
            }
            GroupOffsetCommitSettlementFact::Terminal(input) => {
                GroupOffsetCommitSettlementFact::Terminal(input)
            }
        };
    }

    pub(super) fn into_retry_candidate(self) -> Option<GroupOffsetCommitRetryCandidate> {
        if !matches!(self.coordinator_refresh, CoordinatorRefresh::None) {
            return None;
        }
        match self.fact {
            GroupOffsetCommitSettlementFact::Retry(candidate) => Some(candidate),
            GroupOffsetCommitSettlementFact::Terminal(_) => None,
        }
    }

    pub(super) fn is_retry_ready(&self) -> bool {
        matches!(self.coordinator_refresh, CoordinatorRefresh::None)
            && matches!(self.fact, GroupOffsetCommitSettlementFact::Retry(_))
    }

    pub(super) fn into_parts(
        self,
    ) -> (GroupOffsetCommitInput, PendingGroupOffsetCommitConfirmation) {
        (
            self.fact.into_terminal(),
            PendingGroupOffsetCommitConfirmation {
                operation_id: self.operation_id,
                route_token: self.route_token,
            },
        )
    }

    pub(super) fn recover_group_commit_after_driver_shutdown(
        self,
    ) -> (OperationId, GroupOffsetCommitInput) {
        drop(self.coordinator_refresh);
        drop(self.route_token);
        (self.operation_id, self.fact.into_terminal())
    }
}

enum GroupOffsetCommitSettlementFact {
    Terminal(GroupOffsetCommitInput),
    Retry(GroupOffsetCommitRetryCandidate),
}

impl GroupOffsetCommitSettlementFact {
    fn into_terminal(self) -> GroupOffsetCommitInput {
        match self {
            Self::Terminal(input) => input,
            Self::Retry(candidate) => candidate.into_terminal(),
        }
    }
}

enum CoordinatorRefresh {
    None,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

pub(super) struct PendingGroupOffsetCommitConfirmation {
    operation_id: OperationId,
    route_token: Option<RouteFailureToken>,
}

impl PendingGroupOffsetCommitConfirmation {
    pub(super) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(super) fn into_settled(
        self,
        input: GroupOffsetCommitInput,
    ) -> SettledGroupOffsetCommitCall {
        SettledGroupOffsetCommitCall::new(self.operation_id, input, self.route_token)
    }

    pub(super) fn confirm_group_commit_route_token(self) {
        drop(self.route_token);
    }

    pub(super) fn recover_group_commit_after_driver_shutdown(self) -> OperationId {
        drop(self.route_token);
        self.operation_id
    }
}
