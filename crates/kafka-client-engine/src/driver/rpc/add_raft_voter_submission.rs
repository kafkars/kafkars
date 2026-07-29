//! Single-attempt AnyBroker submission policy for Admin `AddRaftVoter`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{AddRaftVoterRequest, AddRaftVoterResponse};

use super::super::DriverOwner;

const ADD_RAFT_VOTER_MIN_VERSION: ApiVersion = ApiVersion::new(0);
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
    /// Submits one committed voter addition without retry or invalidation policy.
    pub(crate) fn submit_tracked_add_raft_voter(
        &self,
        request: AddRaftVoterRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<AddRaftVoterResponse>, AddRaftVoterSubmitError> {
        self.driver
            .request_tracked_with(
                add_raft_voter_route(),
                request,
                add_raft_voter_options(deadline),
            )
            .map_err(|source| AddRaftVoterSubmitError { source })
    }
}

pub(super) const fn add_raft_voter_route() -> Route {
    Route::AnyBroker
}

pub(super) const fn add_raft_voter_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(ADD_RAFT_VOTER_MIN_VERSION)
        .with_maximum_version(ADD_RAFT_VOTER_MAX_VERSION)
}
