//! Raw versioned Fetch terminal retained without decoding on the driver seam.

use kafka_client_core::{FetchFence, Moment};
use kafka_driver::{ApiVersion, CompletionError, RequestError};
use kafka_wire::FetchResponse as WireFetchResponse;

use super::admission::PartitionFetchRequest;

/// Raw driver terminal and exact prepared ownership for a later outcome owner.
#[must_use = "a raw Fetch terminal retains its exact prepared request"]
pub(crate) struct FetchTerminal {
    request: PartitionFetchRequest,
    observed_at: Moment,
    selected_version: Option<i16>,
    result: Result<WireFetchResponse, RequestError>,
}

impl FetchTerminal {
    pub(crate) const fn fence(&self) -> FetchFence {
        self.request.fence()
    }

    pub(crate) const fn observed_at(&self) -> Moment {
        self.observed_at
    }

    pub(crate) const fn selected_version(&self) -> Option<i16> {
        self.selected_version
    }

    pub(crate) const fn result(&self) -> &Result<WireFetchResponse, RequestError> {
        &self.result
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        PartitionFetchRequest,
        Moment,
        Option<i16>,
        Result<WireFetchResponse, RequestError>,
    ) {
        (
            self.request,
            self.observed_at,
            self.selected_version,
            self.result,
        )
    }

    pub(super) fn into_request(self) -> PartitionFetchRequest {
        self.request
    }
}

/// Completion ownership failure retaining any exact prepared output owner.
#[must_use = "completion failure ownership is released only after driver shutdown"]
pub(crate) struct FetchCompletionFailure {
    request: Option<PartitionFetchRequest>,
    fence: FetchFence,
    source: CompletionError,
}

impl FetchCompletionFailure {
    pub(super) const fn new(
        request: Option<PartitionFetchRequest>,
        fence: FetchFence,
        source: CompletionError,
    ) -> Self {
        Self {
            request,
            fence,
            source,
        }
    }

    pub(crate) const fn observation(&self) -> FetchCompletionObservation {
        FetchCompletionObservation {
            fence: self.fence,
            kind: FetchCompletionKind::from_driver(self.source),
        }
    }

    pub(crate) fn into_parts(self) -> (Option<PartitionFetchRequest>, FetchCompletionObservation) {
        let observation = self.observation();
        (self.request, observation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FetchCompletionKind {
    Closed,
    Consumed,
    Unknown,
}

impl FetchCompletionKind {
    const fn from_driver(source: CompletionError) -> Self {
        match source {
            CompletionError::Closed => Self::Closed,
            CompletionError::Consumed => Self::Consumed,
            _ => Self::Unknown,
        }
    }
}

/// Copyable reactor observation while failure ownership remains retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchCompletionObservation {
    fence: FetchFence,
    kind: FetchCompletionKind,
}

impl FetchCompletionObservation {
    pub(super) const fn from_driver(fence: FetchFence, source: CompletionError) -> Self {
        Self {
            fence,
            kind: FetchCompletionKind::from_driver(source),
        }
    }

    pub(crate) const fn fence(self) -> FetchFence {
        self.fence
    }

    pub(crate) const fn is_consumed(self) -> bool {
        matches!(self.kind, FetchCompletionKind::Consumed)
    }
}

pub(super) fn retain_fetch_terminal(
    request: PartitionFetchRequest,
    observed_at: Moment,
    selected_version: Option<ApiVersion>,
    result: Result<WireFetchResponse, RequestError>,
) -> FetchTerminal {
    FetchTerminal {
        request,
        observed_at,
        selected_version: selected_version.map(ApiVersion::value),
        result,
    }
}
