//! Controller-routed submission of generated automatic `CreatePartitions`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{CreatePartitionsRequest, CreatePartitionsResponse};

use super::super::DriverOwner;

const CREATE_PARTITIONS_MAX_VERSION: ApiVersion = ApiVersion::new(3);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct CreatePartitionsSubmitError {
    source: SubmitError,
}

impl fmt::Display for CreatePartitionsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected CreatePartitions request: {}",
            self.source
        )
    }
}

impl Error for CreatePartitionsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_create_partitions(
        &self,
        request: CreatePartitionsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<CreatePartitionsResponse>, CreatePartitionsSubmitError> {
        self.driver
            .request_tracked_with(
                Route::Controller,
                request,
                create_partitions_options(deadline),
            )
            .map_err(|source| CreatePartitionsSubmitError { source })
    }
}

pub(super) const fn create_partitions_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_maximum_version(CREATE_PARTITIONS_MAX_VERSION)
}
