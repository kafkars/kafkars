//! Terminal event values and bounded-store failure observations.

use std::sync::Arc;

use kafka_client_core::{
    FetchFailure, FetchFence, FetchThrottleFailure, PositionFence, PositionResolutionFailure,
};

/// One application-visible terminal fact transferred out of its active claim.
#[allow(
    clippy::enum_variant_names,
    reason = "variants intentionally preserve the names of the corresponding core terminal effects"
)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerEvent {
    PositionResolutionFailed {
        topic: Arc<str>,
        fence: PositionFence,
        failure: PositionResolutionFailure,
    },
    FetchThrottleFailed {
        topic: Arc<str>,
        fence: FetchFence,
        failure: FetchThrottleFailure,
    },
    FetchFailed {
        topic: Arc<str>,
        fence: FetchFence,
        failure: FetchFailure,
    },
}

/// Failure before the fixed-capacity event owner exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerEventStoreBuildError {
    Allocation,
}

/// Lossless rejection of an event-capacity or fence transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerEventStoreError {
    Capacity,
    ClaimMissing,
    ClaimMismatch,
    TransitionMismatch,
}

/// Scalar cleanup evidence after the driver can no longer produce terminal facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AssignedConsumerEventRecovery {
    claimed: usize,
    ready: usize,
}

impl AssignedConsumerEventRecovery {
    pub(super) const fn new(claimed: usize, ready: usize) -> Self {
        Self { claimed, ready }
    }

    pub(crate) const fn claimed(self) -> usize {
        self.claimed
    }

    pub(crate) const fn ready(self) -> usize {
        self.ready
    }
}
