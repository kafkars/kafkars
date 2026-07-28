//! Controller-routed submission of generated leader election.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::LeaderElectionType;
use kafka_driver::{RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{ElectLeadersRequest, ElectLeadersResponse};

use crate::protocol::admin::elect_leaders::{ELECT_LEADERS_MAX_VERSION, minimum_version};

use super::super::DriverOwner;

/// Definitely-unsent failure before driver request ownership.
#[derive(Debug)]
pub(crate) struct ElectLeadersSubmitError {
    source: SubmitError,
}

impl fmt::Display for ElectLeadersSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected ElectLeaders request: {}",
            self.source
        )
    }
}

impl Error for ElectLeadersSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    pub(crate) fn submit_tracked_elect_leaders(
        &self,
        request: ElectLeadersRequest,
        election_type: LeaderElectionType,
        deadline: Instant,
    ) -> Result<RoutedCall<ElectLeadersResponse>, ElectLeadersSubmitError> {
        self.driver
            .request_tracked_with(
                Route::Controller,
                request,
                elect_leaders_options(election_type, deadline),
            )
            .map_err(|source| ElectLeadersSubmitError { source })
    }
}

pub(super) fn elect_leaders_options(
    election_type: LeaderElectionType,
    deadline: Instant,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(kafka_driver::ApiVersion::new(
            minimum_version(election_type).value(),
        ))
        .with_maximum_version(kafka_driver::ApiVersion::new(
            ELECT_LEADERS_MAX_VERSION.value(),
        ))
}
