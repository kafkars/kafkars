//! Exact failure facts retaining one share-fetch partition route request.

use crate::driver::TopicPartitionCountFailure;

use super::ShareFetchPartitionRouteRequest;

/// Route failure retaining exact assignment ownership.
#[must_use = "a failed share partition route must be retried or released"]
pub(in crate::consumer::share) struct ShareFetchPartitionRouteFailure {
    request: ShareFetchPartitionRouteRequest,
    kind: ShareFetchPartitionRouteFailureKind,
}

impl ShareFetchPartitionRouteFailure {
    pub(in crate::consumer::share) const fn new(
        request: ShareFetchPartitionRouteRequest,
        kind: ShareFetchPartitionRouteFailureKind,
    ) -> Self {
        Self { request, kind }
    }

    pub(in crate::consumer::share) const fn kind(&self) -> ShareFetchPartitionRouteFailureKind {
        self.kind
    }

    pub(in crate::consumer::share) fn into_request(self) -> ShareFetchPartitionRouteRequest {
        self.request
    }

    pub(in crate::consumer::share) fn with_kind(
        self,
        kind: ShareFetchPartitionRouteFailureKind,
    ) -> Self {
        Self::new(self.request, kind)
    }

    #[cfg(test)]
    pub(in crate::consumer::share) const fn for_test(
        request: ShareFetchPartitionRouteRequest,
        kind: ShareFetchPartitionRouteFailureKind,
    ) -> Self {
        Self::new(request, kind)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareFetchPartitionRouteFailureKind {
    Unassigned,
    UnknownTopic,
    Deadline,
    Backpressured,
    DriverRejected,
    TopicIdentityChanged,
    LeaderUnavailable,
    InvalidBroker,
    TopicView(TopicPartitionCountFailure),
}

impl ShareFetchPartitionRouteFailureKind {
    pub(in crate::consumer::share) const fn is_transient_metadata(self) -> bool {
        matches!(
            self,
            Self::LeaderUnavailable
                | Self::TopicView(
                    TopicPartitionCountFailure::Unavailable
                        | TopicPartitionCountFailure::Refresh
                        | TopicPartitionCountFailure::Broker(3 | 5)
                )
        )
    }
}
