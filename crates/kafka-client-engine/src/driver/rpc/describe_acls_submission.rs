//! Any-broker submission policy for Admin `DescribeAcls`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeAclsRequest, DescribeAclsResponse};

use super::super::DriverOwner;

const DESCRIBE_ACLS_MIN_VERSION: ApiVersion = ApiVersion::new(1);
const DESCRIBE_ACLS_MAX_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DescribeAclsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeAclsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeAcls request: {}",
            self.source
        )
    }
}

impl Error for DescribeAclsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one read-only ACL filter through an arbitrary broker.
    pub(crate) fn submit_describe_acls(
        &self,
        request: DescribeAclsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeAclsResponse>, DescribeAclsSubmitError> {
        self.driver
            .request_tracked_with(
                describe_acls_route(),
                request,
                describe_acls_options(deadline),
            )
            .map_err(|source| DescribeAclsSubmitError { source })
    }
}

pub(super) const fn describe_acls_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_acls_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_ACLS_MIN_VERSION)
        .with_maximum_version(DESCRIBE_ACLS_MAX_VERSION)
}
