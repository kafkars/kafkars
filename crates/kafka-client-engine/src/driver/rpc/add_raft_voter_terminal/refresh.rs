//! One-shot controller-route invalidation retained beside a known API 80 terminal.

use std::mem;

use kafka_driver::{Call, InvalidationDisposition, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::AddRaftVoterResponse;

use super::response_requires_controller_refresh;
use crate::driver::DriverOwner;

pub(super) enum AddRaftVoterControllerRefresh {
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

impl AddRaftVoterControllerRefresh {
    pub(super) fn from_terminal(
        selected_version: Option<i16>,
        result: &Result<AddRaftVoterResponse, RequestError>,
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

    pub(super) fn poll(&mut self, driver: Option<&DriverOwner>) -> Option<bool> {
        match mem::replace(self, Self::None) {
            Self::None => Some(true),
            Self::Queued(route_token) => {
                let Some(driver) = driver else {
                    *self = Self::Queued(route_token);
                    return None;
                };
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued(route_token);
                    }
                }
                Some(false)
            }
            Self::Active(call) => {
                if call.try_result().is_none() {
                    *self = Self::Active(call);
                    Some(false)
                } else {
                    Some(true)
                }
            }
            #[cfg(test)]
            Self::QueuedForTest => {
                if driver.is_none() {
                    *self = Self::QueuedForTest;
                    None
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: false,
                    };
                    Some(false)
                }
            }
            #[cfg(test)]
            Self::ActiveForTest { completion_ready } => {
                if completion_ready {
                    Some(true)
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: true,
                    };
                    Some(false)
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn queued_for_test() -> Self {
        Self::QueuedForTest
    }

    #[cfg(test)]
    pub(super) fn arm_for_test(&mut self) {
        *self = Self::QueuedForTest;
    }
}
