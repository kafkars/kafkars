//! Any-broker discovery and exact-broker submission policy for Admin `ListTransactions`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, BrokerId, BrokerIdError, RequestOptions, Route, RoutedCall, SubmitError,
    TrafficClass,
};
use kafka_wire::{
    DescribeClusterRequest, DescribeClusterResponse, ListTransactionsRequest,
    ListTransactionsResponse,
};

use super::super::DriverOwner;

const DESCRIBE_CLUSTER_MAX_VERSION: ApiVersion = ApiVersion::new(2);
const LIST_TRANSACTIONS_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent discovery, version-floor, route, or driver-admission failure.
#[derive(Debug)]
pub(crate) enum ListTransactionsSubmitError {
    InvalidBroker(BrokerIdError),
    InvalidVersionFloor { actual: i16 },
    Driver(SubmitError),
}

impl fmt::Display for ListTransactionsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(formatter, "invalid ListTransactions broker route: {source}")
            }
            Self::InvalidVersionFloor { actual } => {
                write!(
                    formatter,
                    "invalid ListTransactions API-version floor {actual}"
                )
            }
            Self::Driver(source) => {
                write!(formatter, "driver rejected ListTransactions call: {source}")
            }
        }
    }
}

impl Error for ListTransactionsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
            Self::InvalidVersionFloor { .. } => None,
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_list_transactions_discovery(
        &self,
        request: DescribeClusterRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeClusterResponse>, ListTransactionsSubmitError> {
        self.driver
            .request_tracked_with(
                list_transactions_discovery_route(),
                request,
                list_transactions_discovery_options(deadline),
            )
            .map_err(ListTransactionsSubmitError::Driver)
    }

    pub(super) fn submit_tracked_list_transactions_broker(
        &self,
        broker_id: i32,
        request: ListTransactionsRequest,
        minimum_version: i16,
        deadline: Instant,
    ) -> Result<RoutedCall<ListTransactionsResponse>, ListTransactionsSubmitError> {
        let route = list_transactions_broker_route(broker_id)
            .map_err(ListTransactionsSubmitError::InvalidBroker)?;
        let options = list_transactions_broker_options(deadline, minimum_version)?;
        self.driver
            .request_tracked_with(route, request, options)
            .map_err(ListTransactionsSubmitError::Driver)
    }
}

pub(super) const fn list_transactions_discovery_route() -> Route {
    Route::AnyBroker
}

pub(super) fn list_transactions_broker_route(broker_id: i32) -> Result<Route, BrokerIdError> {
    Ok(Route::Broker {
        broker_id: BrokerId::new(broker_id)?,
    })
}

pub(super) const fn list_transactions_discovery_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DESCRIBE_CLUSTER_MAX_VERSION)
}

pub(super) fn list_transactions_broker_options(
    deadline: Instant,
    minimum_version: i16,
) -> Result<RequestOptions, ListTransactionsSubmitError> {
    if !(0..=LIST_TRANSACTIONS_MAX_VERSION.value()).contains(&minimum_version) {
        return Err(ListTransactionsSubmitError::InvalidVersionFloor {
            actual: minimum_version,
        });
    }
    Ok(RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(minimum_version))
        .with_maximum_version(LIST_TRANSACTIONS_MAX_VERSION))
}
