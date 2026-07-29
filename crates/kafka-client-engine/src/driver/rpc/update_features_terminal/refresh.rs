//! One-shot controller-route invalidation retained beside a known API 57 terminal.

use std::mem;

use kafka_driver::{Call, InvalidationDisposition, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::UpdateFeaturesResponse;

use super::response_requires_controller_refresh;
use crate::driver::DriverOwner;

pub(super) enum UpdateFeaturesControllerRefresh {
    None,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    #[cfg(test)]
    QueuedForTest,
    #[cfg(test)]
    ActiveForTest {
        completion_ready: bool,
    },
}

/// One bounded observation of the causal controller invalidation barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesControllerRefreshPoll {
    Ready,
    Pending,
    DriverMissing,
}

impl UpdateFeaturesControllerRefresh {
    pub(super) fn from_terminal(
        selected_version: Option<i16>,
        result: &Result<UpdateFeaturesResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        if response_requires_controller_refresh(selected_version, result) {
            match route_token {
                Some(route_token) if route_token.kind() == RouteKind::Controller => {
                    Self::Queued(route_token)
                }
                route_token => {
                    drop(route_token);
                    Self::None
                }
            }
        } else {
            drop(route_token);
            Self::None
        }
    }

    pub(super) fn poll(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> UpdateFeaturesControllerRefreshPoll {
        match mem::replace(self, Self::None) {
            Self::None => UpdateFeaturesControllerRefreshPoll::Ready,
            Self::Queued(route_token) => {
                let Some(driver) = driver else {
                    *self = Self::Queued(route_token);
                    return UpdateFeaturesControllerRefreshPoll::DriverMissing;
                };
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued(route_token);
                    }
                }
                UpdateFeaturesControllerRefreshPoll::Pending
            }
            Self::Active(call) => {
                if call.try_result().is_none() {
                    *self = Self::Active(call);
                    UpdateFeaturesControllerRefreshPoll::Pending
                } else {
                    UpdateFeaturesControllerRefreshPoll::Ready
                }
            }
            #[cfg(test)]
            Self::QueuedForTest => {
                if driver.is_none() {
                    *self = Self::QueuedForTest;
                    UpdateFeaturesControllerRefreshPoll::DriverMissing
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: false,
                    };
                    UpdateFeaturesControllerRefreshPoll::Pending
                }
            }
            #[cfg(test)]
            Self::ActiveForTest { completion_ready } => {
                if completion_ready {
                    UpdateFeaturesControllerRefreshPoll::Ready
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: true,
                    };
                    UpdateFeaturesControllerRefreshPoll::Pending
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn arm_for_test(&mut self) {
        *self = Self::QueuedForTest;
    }
}
