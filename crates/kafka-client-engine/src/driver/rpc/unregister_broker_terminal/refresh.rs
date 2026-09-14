//! One-shot controller invalidation and replay authority for broker unregistration.

use std::mem;

use kafka_driver::{Call, InvalidationDisposition, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::UnregisterBrokerResponse;

use super::{request_requires_controller_retry, response_requires_controller_refresh};
use crate::driver::DriverOwner;

pub(super) enum UnregisterBrokerControllerRefresh {
    None,
    Queued {
        route_token: RouteFailureToken,
        retry: bool,
    },
    Active {
        call: Call<InvalidationDisposition>,
        retry: bool,
    },
    #[cfg(test)]
    QueuedForTest {
        retry: bool,
    },
    #[cfg(test)]
    ActiveForTest {
        completion_ready: bool,
        retry: bool,
    },
}

/// Progress of the causal controller-refresh barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerControllerRefreshPoll {
    Ready,
    RetryReady,
    Pending,
    DriverMissing,
}

impl UnregisterBrokerControllerRefresh {
    pub(super) fn from_terminal(
        selected_version: Option<i16>,
        result: &Result<UnregisterBrokerResponse, RequestError>,
        route_token: Option<RouteFailureToken>,
    ) -> Self {
        let retry = request_requires_controller_retry(result);
        if retry || response_requires_controller_refresh(selected_version, result) {
            match route_token {
                Some(route_token) if route_token.kind() == RouteKind::Controller => {
                    Self::Queued { route_token, retry }
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
    ) -> UnregisterBrokerControllerRefreshPoll {
        match mem::replace(self, Self::None) {
            Self::None => UnregisterBrokerControllerRefreshPoll::Ready,
            Self::Queued { route_token, retry } => {
                let Some(driver) = driver else {
                    *self = Self::Queued { route_token, retry };
                    return UnregisterBrokerControllerRefreshPoll::DriverMissing;
                };
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active { call, retry },
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued { route_token, retry };
                    }
                }
                UnregisterBrokerControllerRefreshPoll::Pending
            }
            Self::Active { call, retry } => {
                let Some(result) = call.try_result() else {
                    *self = Self::Active { call, retry };
                    return UnregisterBrokerControllerRefreshPoll::Pending;
                };
                if retry
                    && matches!(
                        result,
                        Ok(InvalidationDisposition::Applied
                            | InvalidationDisposition::IgnoredStale)
                    )
                {
                    UnregisterBrokerControllerRefreshPoll::RetryReady
                } else {
                    UnregisterBrokerControllerRefreshPoll::Ready
                }
            }
            #[cfg(test)]
            Self::QueuedForTest { retry } => {
                if driver.is_none() {
                    *self = Self::QueuedForTest { retry };
                    UnregisterBrokerControllerRefreshPoll::DriverMissing
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: false,
                        retry,
                    };
                    UnregisterBrokerControllerRefreshPoll::Pending
                }
            }
            #[cfg(test)]
            Self::ActiveForTest {
                completion_ready,
                retry,
            } => {
                if completion_ready {
                    if retry {
                        UnregisterBrokerControllerRefreshPoll::RetryReady
                    } else {
                        UnregisterBrokerControllerRefreshPoll::Ready
                    }
                } else {
                    *self = Self::ActiveForTest {
                        completion_ready: true,
                        retry,
                    };
                    UnregisterBrokerControllerRefreshPoll::Pending
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) const fn for_test(retry: bool) -> Self {
        Self::QueuedForTest { retry }
    }

    #[cfg(test)]
    pub(super) fn arm_for_test(&mut self, retry: bool) {
        *self = Self::for_test(retry);
    }
}
