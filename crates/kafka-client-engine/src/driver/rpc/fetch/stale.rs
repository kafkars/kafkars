//! Explicit ownership returned when direct-consumer control supersedes Fetch work.

use kafka_client_core::FetchFence;

use super::{admission::PartitionFetchRequest, terminal::FetchCompletionObservation};

/// Exact prepared requests released by one accepted stale-control transition.
#[must_use = "stale Fetch output ownership must be returned to its executor"]
pub(crate) struct StaleFetchDrains {
    requests: Vec<PartitionFetchRequest>,
}

impl StaleFetchDrains {
    pub(super) const fn new() -> Self {
        Self {
            requests: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, request: PartitionFetchRequest) {
        self.requests.push(request);
    }

    pub(crate) fn into_requests(self) -> Vec<PartitionFetchRequest> {
        self.requests
    }
}

/// Control must be retried after an in-progress two-phase settlement finishes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchControlPending {
    pub(crate) fence: FetchFence,
}

/// Explicit post-driver-shutdown release of otherwise-unsettleable ownership.
#[must_use = "recovered Fetch requests and completion failure must be handled"]
pub(crate) struct FetchRecovery {
    requests: Vec<PartitionFetchRequest>,
    completion_failure: Option<FetchCompletionObservation>,
}

impl FetchRecovery {
    pub(crate) fn new(
        requests: Vec<PartitionFetchRequest>,
        completion_failure: Option<FetchCompletionObservation>,
    ) -> Self {
        Self {
            requests,
            completion_failure,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<PartitionFetchRequest>,
        Option<FetchCompletionObservation>,
    ) {
        (self.requests, self.completion_failure)
    }
}
