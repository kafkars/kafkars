//! Controller-route invalidation retained before one `CreatePartitions` terminal publication.

use std::mem;

use kafka_client_core::{CreatePartitionsInput, OperationId};
use kafka_driver::{Call, InvalidationDisposition, RequestError, RouteFailureToken};
use kafka_wire::CreatePartitionsResponse;

use super::super::DriverOwner;

pub(super) enum CreatePartitionsControllerRefresh {
    None,
    Queued(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreatePartitionsControllerRefreshPoll {
    Ready,
    Pending,
    DriverMissing,
}

pub(crate) struct SettledCreatePartitionsCall {
    operation_id: OperationId,
    input: Option<CreatePartitionsInput>,
    controller_refresh: CreatePartitionsControllerRefresh,
}

impl SettledCreatePartitionsCall {
    pub(super) fn new(
        operation_id: OperationId,
        input: CreatePartitionsInput,
        route_token: Option<RouteFailureToken>,
        broker_requires_controller_refresh: bool,
    ) -> Self {
        let controller_refresh = if broker_requires_controller_refresh
            && matches!(input, CreatePartitionsInput::BrokerResponded { .. })
        {
            route_token.map_or(
                CreatePartitionsControllerRefresh::None,
                CreatePartitionsControllerRefresh::Queued,
            )
        } else {
            drop(route_token);
            CreatePartitionsControllerRefresh::None
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

    pub(crate) fn take_input(&mut self) -> Option<CreatePartitionsInput> {
        self.input.take()
    }

    pub(crate) fn poll_controller_refresh(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> CreatePartitionsControllerRefreshPoll {
        self.controller_refresh.poll(driver)
    }

    pub(super) fn discard(self) {
        drop(self.controller_refresh);
    }

    #[cfg(test)]
    pub(super) fn from_input_for_test(input: CreatePartitionsInput) -> Self {
        Self {
            operation_id: OperationId::from_raw(1),
            input: Some(input),
            controller_refresh: CreatePartitionsControllerRefresh::None,
        }
    }
}

impl CreatePartitionsControllerRefresh {
    pub(super) fn poll(
        &mut self,
        driver: Option<&DriverOwner>,
    ) -> CreatePartitionsControllerRefreshPoll {
        match mem::replace(self, Self::None) {
            Self::None => CreatePartitionsControllerRefreshPoll::Ready,
            Self::Queued(route_token) => {
                let Some(driver) = driver else {
                    *self = Self::Queued(route_token);
                    return CreatePartitionsControllerRefreshPoll::DriverMissing;
                };
                match driver.driver.invalidate(route_token) {
                    Ok(call) => *self = Self::Active(call),
                    Err(rejection) => {
                        let (_source, route_token) = rejection.into_parts();
                        *self = Self::Queued(route_token);
                    }
                }
                CreatePartitionsControllerRefreshPoll::Pending
            }
            Self::Active(call) => {
                if call.try_result().is_none() {
                    *self = Self::Active(call);
                    CreatePartitionsControllerRefreshPoll::Pending
                } else {
                    CreatePartitionsControllerRefreshPoll::Ready
                }
            }
        }
    }
}

pub(super) fn response_requires_controller_refresh(
    result: &Result<CreatePartitionsResponse, RequestError>,
) -> bool {
    matches!(
        result,
        Ok(response) if response.results.iter().any(|result| result.error_code == 41)
    )
}
