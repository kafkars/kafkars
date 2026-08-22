//! Controller-discovery and exact-broker submission policy for group listing.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, BrokerId, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass,
};
use kafka_wire::{
    DescribeClusterRequest, DescribeClusterResponse, ListGroupsRequest, ListGroupsResponse,
};

use super::super::DriverOwner;

const DESCRIBE_CLUSTER_MAX_VERSION: ApiVersion = ApiVersion::new(2);
const LIST_GROUPS_MAX_VERSION: ApiVersion = ApiVersion::new(5);

/// Definitely-unsent discovery or broker-route construction failure.
#[derive(Debug)]
pub(crate) enum ListConsumerGroupsSubmitError {
    InvalidBroker(InvalidBroker),
    Driver(SubmitError),
}

impl fmt::Display for ListConsumerGroupsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBroker(source) => {
                write!(formatter, "invalid ListGroups broker route: {source}")
            }
            Self::Driver(source) => {
                write!(
                    formatter,
                    "driver rejected ListConsumerGroups call: {source}"
                )
            }
        }
    }
}

impl Error for ListConsumerGroupsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBroker(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_list_consumer_groups_discovery(
        &self,
        request: DescribeClusterRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeClusterResponse>, ListConsumerGroupsSubmitError> {
        self.driver
            .request_tracked_with(
                list_consumer_groups_discovery_route(),
                request,
                list_consumer_groups_discovery_options(deadline),
            )
            .map_err(ListConsumerGroupsSubmitError::Driver)
    }

    pub(super) fn submit_tracked_list_consumer_groups_broker(
        &self,
        broker_id: i32,
        request: ListGroupsRequest,
        minimum_version: i16,
        deadline: Instant,
    ) -> Result<RoutedCall<ListGroupsResponse>, ListConsumerGroupsSubmitError> {
        let route = list_consumer_groups_broker_route(broker_id)
            .map_err(ListConsumerGroupsSubmitError::InvalidBroker)?;
        self.driver
            .request_tracked_with(
                route,
                request,
                list_consumer_groups_broker_options(deadline, minimum_version),
            )
            .map_err(ListConsumerGroupsSubmitError::Driver)
    }
}

pub(super) const fn list_consumer_groups_discovery_route() -> Route {
    Route::Controller
}

pub(super) fn list_consumer_groups_broker_route(broker_id: i32) -> Result<Route, InvalidBroker> {
    let broker_id = BrokerId::new(broker_id).map_err(|_error| InvalidBroker(broker_id))?;
    Ok(Route::Broker { broker_id })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvalidBroker(i32);

impl fmt::Display for InvalidBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker ID {} must be nonnegative", self.0)
    }
}

impl Error for InvalidBroker {}

pub(super) const fn list_consumer_groups_discovery_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DESCRIBE_CLUSTER_MAX_VERSION)
}

pub(super) const fn list_consumer_groups_broker_options(
    deadline: Instant,
    minimum_version: i16,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(minimum_version))
        .with_maximum_version(LIST_GROUPS_MAX_VERSION)
}
