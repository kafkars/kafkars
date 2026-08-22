//! Driver-capability owner for one causal broker-route invalidation.

use kafka_driver::{Call, InvalidationDisposition, RouteFailureToken, RouteKind, SubmitError};

use super::super::super::DriverOwner;

pub(crate) enum FetchRouteRefresh {
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchRouteRefreshPoll {
    Pending,
    Ready,
    Failed,
}

impl FetchRouteRefresh {
    pub(crate) fn from_token(token: Option<RouteFailureToken>) -> Option<Self> {
        token
            .filter(|token| token.kind() == RouteKind::Broker)
            .map(Self::Queued)
    }

    pub(crate) fn poll(&mut self, driver: &DriverOwner) -> FetchRouteRefreshPoll {
        match std::mem::replace(self, Self::Unavailable) {
            Self::Queued(token) => match driver.driver.invalidate(token) {
                Ok(call) => {
                    *self = Self::Active(call);
                    FetchRouteRefreshPoll::Pending
                }
                Err(rejection) if matches!(rejection.reason(), SubmitError::Full) => {
                    let (_error, token) = rejection.into_parts();
                    *self = Self::Queued(token);
                    FetchRouteRefreshPoll::Pending
                }
                Err(_rejection) => FetchRouteRefreshPoll::Failed,
            },
            Self::Active(call) => match call.try_result() {
                None => {
                    *self = Self::Active(call);
                    FetchRouteRefreshPoll::Pending
                }
                Some(Ok(
                    InvalidationDisposition::Applied | InvalidationDisposition::IgnoredStale,
                )) => FetchRouteRefreshPoll::Ready,
                Some(Ok(_) | Err(_)) => FetchRouteRefreshPoll::Failed,
            },
            Self::Unavailable => FetchRouteRefreshPoll::Failed,
        }
    }
}
