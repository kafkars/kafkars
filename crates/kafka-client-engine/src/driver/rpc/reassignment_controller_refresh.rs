//! Exact controller-route refresh ownership shared by the two reassignment RPC lanes.

use std::mem;

use kafka_driver::{Call, InvalidationDisposition, RouteFailureToken};

use super::super::DriverOwner;

pub(super) enum ReassignmentControllerRefresh {
    Unclassified(Option<RouteFailureToken>),
    Ready(Option<RouteFailureToken>),
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    Refreshed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReassignmentControllerRefreshPrepareError {
    AlreadyPrepared,
    MissingRouteToken,
}

impl ReassignmentControllerRefresh {
    pub(super) const fn unclassified(route_token: Option<RouteFailureToken>) -> Self {
        Self::Unclassified(route_token)
    }

    pub(super) fn prepare(
        &mut self,
        required: bool,
    ) -> Result<(), ReassignmentControllerRefreshPrepareError> {
        match mem::replace(self, Self::Refreshed) {
            Self::Unclassified(route_token) if !required => {
                *self = Self::Ready(route_token);
                Ok(())
            }
            Self::Unclassified(Some(route_token)) => {
                *self = Self::Queued(route_token);
                Ok(())
            }
            Self::Unclassified(None) => {
                *self = Self::Unclassified(None);
                Err(ReassignmentControllerRefreshPrepareError::MissingRouteToken)
            }
            other => {
                *self = other;
                Err(ReassignmentControllerRefreshPrepareError::AlreadyPrepared)
            }
        }
    }

    pub(super) const fn is_pending(&self) -> bool {
        matches!(self, Self::Queued(_) | Self::Active(_))
    }

    pub(super) fn poll(&mut self, driver: &DriverOwner) -> bool {
        match mem::replace(self, Self::Refreshed) {
            Self::Ready(route_token) => {
                *self = Self::Ready(route_token);
                true
            }
            Self::Refreshed => true,
            Self::Queued(route_token) => {
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued(route_token);
                    }
                }
                false
            }
            Self::Active(call) => {
                if call.try_result().is_some() {
                    true
                } else {
                    *self = Self::Active(call);
                    false
                }
            }
            Self::Unclassified(route_token) => {
                *self = Self::Unclassified(route_token);
                false
            }
        }
    }

    pub(super) fn discard_after_driver_shutdown(self) {
        drop(self);
    }
}
