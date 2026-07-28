//! Controller-routed submission of generated `ListPartitionReassignments` v0.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse};

use super::super::DriverOwner;

const LIST_PARTITION_REASSIGNMENTS_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct ListPartitionReassignmentsSubmitError {
    source: SubmitError,
}

impl fmt::Display for ListPartitionReassignmentsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected ListPartitionReassignments request: {}",
            self.source
        )
    }
}

impl Error for ListPartitionReassignmentsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(super) fn submit_tracked_list_partition_reassignments(
        &self,
        request: ListPartitionReassignmentsRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<ListPartitionReassignmentsResponse>, ListPartitionReassignmentsSubmitError>
    {
        self.driver
            .request_tracked_with(
                Route::Controller,
                request,
                list_partition_reassignments_options(deadline),
            )
            .map_err(|source| ListPartitionReassignmentsSubmitError { source })
    }
}

pub(super) const fn list_partition_reassignments_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(LIST_PARTITION_REASSIGNMENTS_VERSION)
        .with_maximum_version(LIST_PARTITION_REASSIGNMENTS_VERSION)
}
