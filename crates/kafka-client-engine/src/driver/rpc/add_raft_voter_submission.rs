//! Single-attempt AnyBroker submission policy for Admin `AddRaftVoter`.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::AddRaftVoterPlan;
use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{AddRaftVoterRequest, AddRaftVoterResponse};

use super::super::DriverOwner;

const ADD_RAFT_VOTER_MAX_VERSION: ApiVersion = ApiVersion::new(1);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct AddRaftVoterSubmitError {
    source: SubmitError,
}

impl fmt::Display for AddRaftVoterSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected AddRaftVoter request: {}",
            self.source
        )
    }
}

impl Error for AddRaftVoterSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one voter addition without retry or invalidation policy.
    pub(crate) fn submit_tracked_add_raft_voter(
        &self,
        plan: &AddRaftVoterPlan,
        request: AddRaftVoterRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AddRaftVoterResponse>, AddRaftVoterSubmitError> {
        self.driver
            .request_tracked_with(
                add_raft_voter_route(),
                request,
                add_raft_voter_options(plan, deadline),
            )
            .map_err(|source| AddRaftVoterSubmitError { source })
    }
}

pub(super) const fn add_raft_voter_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn add_raft_voter_options(
    plan: &AddRaftVoterPlan,
    deadline: Instant,
) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ApiVersion::new(plan.minimum_api_version()))
        .with_maximum_version(ADD_RAFT_VOTER_MAX_VERSION)
}
