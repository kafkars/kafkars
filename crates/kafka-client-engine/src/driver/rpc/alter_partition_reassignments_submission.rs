//! Controller-routed submission of generated reassignment alteration.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse};

use crate::protocol::admin::alter_partition_reassignments::{
    ALTER_PARTITION_REASSIGNMENTS_MAX_VERSION, minimum_version_for_policy,
};

use super::super::DriverOwner;

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct AlterPartitionReassignmentsSubmitError {
    source: SubmitError,
}

impl fmt::Display for AlterPartitionReassignmentsSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected AlterPartitionReassignments request: {}",
            self.source
        )
    }
}

impl Error for AlterPartitionReassignmentsSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_alter_partition_reassignments(
        &self,
        request: AlterPartitionReassignmentsRequest,
        deadline: Instant,
    ) -> Result<
        RoutedCall<AlterPartitionReassignmentsResponse>,
        AlterPartitionReassignmentsSubmitError,
    > {
        let allow_replication_factor_change = request.allow_replication_factor_change;
        self.driver
            .request_tracked_with(
                Route::Controller,
                request,
                alter_partition_reassignments_options(deadline, allow_replication_factor_change),
            )
            .map_err(|source| AlterPartitionReassignmentsSubmitError { source })
    }
}

pub(super) fn alter_partition_reassignments_options(
    deadline: Instant,
    allow_replication_factor_change: bool,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(kafka_driver::ApiVersion::new(
            minimum_version_for_policy(allow_replication_factor_change).value(),
        ))
        .with_maximum_version(kafka_driver::ApiVersion::new(
            ALTER_PARTITION_REASSIGNMENTS_MAX_VERSION.value(),
        ))
}
