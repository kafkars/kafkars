//! Concrete controller-routed submission of one generated `CreateTopics` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{CreateTopicsRequest, CreateTopicsResponse};

use super::super::DriverOwner;

const CREATE_TOPICS_MAX_VERSION: ApiVersion = ApiVersion::new(7);

/// Definitely-unsent failure before the driver accepted request ownership.
#[derive(Debug)]
pub(crate) struct CreateTopicsSubmitError {
    source: SubmitError,
}

impl fmt::Display for CreateTopicsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected CreateTopics request: {}",
            self.source
        )
    }
}

impl Error for CreateTopicsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_create_topics(
        &self,
        request: CreateTopicsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<CreateTopicsResponse>, CreateTopicsSubmitError> {
        self.driver
            .request_tracked_with(Route::Controller, request, create_topics_options(deadline))
            .map_err(|source| CreateTopicsSubmitError { source })
    }
}

pub(super) const fn create_topics_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(CREATE_TOPICS_MAX_VERSION)
}
