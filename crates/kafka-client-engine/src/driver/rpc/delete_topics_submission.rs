//! Controller-routed submission of one generated name-based `DeleteTopics` request.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{DeleteTopicsRequest, DeleteTopicsResponse};

use super::super::DriverOwner;

// Version 6 replaces name-only requests with name-or-ID structs. The public
// model owns names only, so v5 is the newest correlation-safe representation.
const DELETE_TOPICS_MAX_VERSION: ApiVersion = ApiVersion::new(5);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct DeleteTopicsSubmitError {
    source: SubmitError,
}

impl fmt::Display for DeleteTopicsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected DeleteTopics request: {}",
            self.source
        )
    }
}

impl Error for DeleteTopicsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_delete_topics(
        &self,
        request: DeleteTopicsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<DeleteTopicsResponse>, DeleteTopicsSubmitError> {
        self.driver
            .request_tracked_with(Route::Controller, request, delete_topics_options(deadline))
            .map_err(|source| DeleteTopicsSubmitError { source })
    }
}

pub(super) const fn delete_topics_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(DELETE_TOPICS_MAX_VERSION)
}
