//! Deadline-bounded controller evidence settlement before core authorizes a retry.

use std::mem;

use kafka_client_core::{Deadline, Moment};
use kafka_driver::{
    Call, CallFailure, Delivery, InvalidationDisposition, RequestError, RouteFailureToken,
    RouteKind,
};
use kafka_wire::CreateTopicsResponse;

use super::super::DriverOwner;
use crate::clock::OperationDeadline;

pub(super) struct CreateTopicsControllerRefresh {
    deadline: OperationDeadline,
    state: RefreshState,
}

enum RefreshState {
    Unrouted,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    Done,
}

pub(super) enum RefreshPoll {
    Pending,
    RetryReady,
    Failed,
}

impl CreateTopicsControllerRefresh {
    pub(super) fn from_terminal(
        result: &Result<CreateTopicsResponse, RequestError>,
        route_token: &mut Option<RouteFailureToken>,
        deadline: OperationDeadline,
    ) -> Option<Self> {
        if !retryable(result) {
            return None;
        }
        let state = match route_token.as_ref().map(RouteFailureToken::kind) {
            Some(RouteKind::Controller) => RefreshState::Queued(route_token.take()?),
            None if matches!(result, Err(RequestError::RouteUnavailable)) => RefreshState::Unrouted,
            _ => return None,
        };
        Some(Self { deadline, state })
    }

    pub(super) fn poll(&mut self, driver: &DriverOwner, now: Moment) -> RefreshPoll {
        if self.deadline.core().is_elapsed_at(now) {
            self.state = RefreshState::Done;
            // Core assigns the original-deadline terminal without admitting another attempt.
            return RefreshPoll::RetryReady;
        }
        match mem::replace(&mut self.state, RefreshState::Done) {
            RefreshState::Unrouted => RefreshPoll::RetryReady,
            RefreshState::Queued(token) => {
                self.state = match driver.driver.invalidate(token) {
                    Ok(call) => RefreshState::Active(call),
                    Err(rejection) => RefreshState::Queued(rejection.into_parts().1),
                };
                RefreshPoll::Pending
            }
            RefreshState::Active(call) => match call.try_result() {
                None => {
                    self.state = RefreshState::Active(call);
                    RefreshPoll::Pending
                }
                Some(Ok(
                    InvalidationDisposition::Applied | InvalidationDisposition::IgnoredStale,
                )) => RefreshPoll::RetryReady,
                Some(_) => RefreshPoll::Failed,
            },
            RefreshState::Done => RefreshPoll::Failed,
        }
    }

    pub(super) const fn next_deadline(&self) -> Deadline {
        self.deadline.core()
    }
}

pub(super) fn retryable(result: &Result<CreateTopicsResponse, RequestError>) -> bool {
    matches!(
        result,
        Err(RequestError::RouteUnavailable
            | RequestError::Rejected {
                failure: CallFailure::NotReady,
                delivery: Delivery::NotSent,
            })
    )
}
