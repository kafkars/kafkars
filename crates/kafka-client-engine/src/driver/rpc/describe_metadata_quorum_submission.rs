//! Any-broker submission policy for Admin `DescribeMetadataQuorum`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DescribeQuorumRequest, DescribeQuorumResponse};

use super::super::DriverOwner;

const DESCRIBE_QUORUM_MIN_VERSION: ApiVersion = ApiVersion::new(0);
const DESCRIBE_QUORUM_MAX_VERSION: ApiVersion = ApiVersion::new(2);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct DescribeMetadataQuorumSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeMetadataQuorumSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DescribeMetadataQuorum request: {}",
            self.source
        )
    }
}

impl Error for DescribeMetadataQuorumSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one fixed read-only metadata-quorum query through any broker.
    pub(crate) fn submit_describe_metadata_quorum(
        &self,
        request: DescribeQuorumRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DescribeQuorumResponse>, DescribeMetadataQuorumSubmitError> {
        self.driver
            .request_tracked_with(
                describe_metadata_quorum_route(),
                request,
                describe_metadata_quorum_options(deadline),
            )
            .map_err(|source| DescribeMetadataQuorumSubmitError { source })
    }
}

pub(super) const fn describe_metadata_quorum_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_metadata_quorum_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(DESCRIBE_QUORUM_MIN_VERSION)
        .with_maximum_version(DESCRIBE_QUORUM_MAX_VERSION)
}
