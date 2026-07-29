//! Controller-route invalidation retained before one `DeleteTopics` terminal publication.

use std::mem;

use kafka_client_core::{DeleteTopicsInput, OperationId};
use kafka_driver::{Call, InvalidationDisposition, RequestError, RouteFailureToken};
use kafka_wire::DeleteTopicsResponse;

use super::super::DriverOwner;

pub(super) enum DeleteTopicsControllerRefresh {
    None,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteTopicsControllerRefreshPoll {
    Ready,
    Pending,
    DriverMissing,
}

pub(crate) struct SettledDeleteTopicsCall {
    operation_id: OperationId,
    input: Option<DeleteTopicsInput>,
    controller_refresh: DeleteTopicsControllerRefresh,
}

impl SettledDeleteTopicsCall {
    pub(super) fn new(
        operation_id: OperationId,
        input: DeleteTopicsInput,
        route_token: Option<RouteFailureToken>,
        broker_requires_controller_refresh: bool,
    ) -> Self {
        let controller_refresh = if normalized_response_requires_controller_refresh(
            &input,
            broker_requires_controller_refresh,
        ) {
            route_token.map_or(
                DeleteTopicsControllerRefresh::None,
                DeleteTopicsControllerRefresh::Queued,
            )
        } else {
            drop(route_token);
            DeleteTopicsControllerRefresh::None
        };
        Self {
            operation_id,
            input: Some(input),
            controller_refresh,
        }
    }

    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<DeleteTopicsInput> {
        self.input.take()
    }

    pub(crate) fn poll_controller_refresh(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> DeleteTopicsControllerRefreshPoll {
        self.controller_refresh.poll(driver)
    }

    pub(super) fn discard(self) {
        drop(self.controller_refresh);
    }

    #[cfg(test)]
    pub(super) fn from_input_for_test(input: DeleteTopicsInput) -> Self {
        Self {
            operation_id: OperationId::from_raw(1),
            input: Some(input),
            controller_refresh: DeleteTopicsControllerRefresh::None,
        }
    }
}

impl DeleteTopicsControllerRefresh {
    pub(super) fn poll(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> DeleteTopicsControllerRefreshPoll {
        match mem::replace(self, Self::None) {
            Self::None => DeleteTopicsControllerRefreshPoll::Ready,
            Self::Queued(route_token) => {
                let Some(driver) = driver else {
                    *self = Self::Queued(route_token);
                    return DeleteTopicsControllerRefreshPoll::DriverMissing;
                };
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued(route_token);
                    }
                }
                DeleteTopicsControllerRefreshPoll::Pending
            }
            Self::Active(call) => {
                if call.try_result().is_none() {
                    *self = Self::Active(call);
                    DeleteTopicsControllerRefreshPoll::Pending
                } else {
                    DeleteTopicsControllerRefreshPoll::Ready
                }
            }
        }
    }
}

pub(super) fn response_requires_controller_refresh(
    result: &Result<DeleteTopicsResponse, RequestError>,
) -> bool {
    matches!(
        result,
        Ok(response) if response.responses.iter().any(|result| result.error_code == 41)
    )
}

pub(super) fn normalized_response_requires_controller_refresh(
    input: &DeleteTopicsInput,
    broker_requires_controller_refresh: bool,
) -> bool {
    broker_requires_controller_refresh
        && matches!(
            input,
            DeleteTopicsInput::BrokerResponded { .. }
                | DeleteTopicsInput::BrokerRespondedById { .. }
        )
}
