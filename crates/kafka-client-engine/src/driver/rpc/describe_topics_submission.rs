//! Any-broker submission of one batched generated Metadata request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{
    ApiVersion, Call, RequestError, RequestOptions, Route, SubmitError, TrafficClass,
};
use kafka_wire::{MetadataRequest, MetadataResponse};

use super::super::DriverOwner;

const DESCRIBE_TOPICS_MAX_VERSION: ApiVersion = ApiVersion::new(13);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct DescribeTopicsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DescribeTopicsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected transient Metadata request: {}",
            self.source
        )
    }
}

impl Error for DescribeTopicsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_describe_topics(
        &self,
        request: MetadataRequest,
        deadline: Instant,
    ) -> Result<Call<Result<MetadataResponse, RequestError>>, DescribeTopicsSubmitError> {
        self.driver
            .request_with(
                describe_topics_route(),
                request,
                describe_topics_options(deadline),
            )
            .map_err(|source| DescribeTopicsSubmitError { source })
    }
}

pub(super) const fn describe_topics_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn describe_topics_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DESCRIBE_TOPICS_MAX_VERSION)
}
