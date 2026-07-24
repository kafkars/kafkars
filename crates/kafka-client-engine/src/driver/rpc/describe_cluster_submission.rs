//! Any-broker submission of one generated `DescribeCluster` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, Call, RequestError, RequestOptions, Route, SubmitError, TrafficClass,
};
use kafka_wire::{DescribeClusterRequest, DescribeClusterResponse};

use super::super::DriverOwner;

const DESCRIBE_CLUSTER_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct DescribeClusterSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeClusterSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeCluster request: {}",
            self.source
        )
    }
}

impl Error for DescribeClusterSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_describe_cluster(
        &self,
        request: DescribeClusterRequest,
        deadline: Instant,
        _include_fenced_brokers: bool,
        _include_authorized_operations: bool,
    ) -> Result<Call<Result<DescribeClusterResponse, RequestError>>, DescribeClusterSubmitError>
    {
        self.driver
            .request_with(
                describe_cluster_route(),
                request,
                describe_cluster_options(deadline, false, false),
            )
            .map_err(|source| DescribeClusterSubmitError { source })
    }
}

pub(super) const fn describe_cluster_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_cluster_options(
    deadline: Instant,
    _include_fenced_brokers: bool,
    _include_authorized_operations: bool,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DESCRIBE_CLUSTER_MAX_VERSION)
}
