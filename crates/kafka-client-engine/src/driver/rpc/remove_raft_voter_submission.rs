//! Single-attempt controller submission policy for Admin `RemoveRaftVoter`.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{ApiVersion, RequestOptions, Route, RoutedCall, SubmitError, TrafficClass};
use kafka_wire::{RemoveRaftVoterRequest, RemoveRaftVoterResponse};

use super::super::DriverOwner;

const REMOVE_RAFT_VOTER_VERSION: ApiVersion = ApiVersion::new(0);

/// Definitely-unsent bounded-driver rejection.
#[derive(Debug)]
pub(crate) struct RemoveRaftVoterSubmitError {
    source: SubmitError,
}

impl fmt::Display for RemoveRaftVoterSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "driver rejected RemoveRaftVoter request: {}",
            self.source
        )
    }
}

impl Error for RemoveRaftVoterSubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl DriverOwner {
    /// Submits one voter removal without automatic replay policy.
    pub(crate) fn submit_tracked_remove_raft_voter(
        &self,
        request: RemoveRaftVoterRequest,
        deadline: Instant,
    ) -> Result<RoutedCall<RemoveRaftVoterResponse>, RemoveRaftVoterSubmitError> {
        self.driver
            .request_tracked_with(
                remove_raft_voter_route(),
                request,
                remove_raft_voter_options(deadline),
            )
            .map_err(|source| RemoveRaftVoterSubmitError { source })
    }
}

pub(super) const fn remove_raft_voter_route() -> Route {
    Route::Controller
}

pub(super) const fn remove_raft_voter_options(deadline: Instant) -> RequestOptions {
    RequestOptions::new(deadline)
        .with_traffic_class(TrafficClass::Interactive)
        .with_minimum_version(REMOVE_RAFT_VOTER_VERSION)
        .with_maximum_version(REMOVE_RAFT_VOTER_VERSION)
}
