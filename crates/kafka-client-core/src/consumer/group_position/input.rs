//! Closed assignment-fenced facts accepted by group position bootstrap.

use core::fmt;

use crate::Moment;

use super::{GroupPositionBatch, GroupPositionBrokerError, GroupPositionFence};

/// Driver or protocol terminal outside a correlated `OffsetFetch` response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapFetchFailure {
    /// Driver-owned transport execution failed.
    Transport,
    /// The selected version cannot represent the bootstrap request.
    Compatibility,
    /// The generated response was malformed.
    InvalidResponse,
    /// The response exceeded the retained result bound.
    ResponseTooLarge,
}

/// One normalized fact for a single assignment bootstrap.
#[derive(Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapInput {
    /// Starts this exact assignment bootstrap.
    Start {
        /// Assignment fence authorizing the start.
        fence: GroupPositionFence,
        /// Current monotonic observation.
        now: Moment,
    },
    /// The driver accepted the sole `OffsetFetch` request.
    DriverAccepted {
        /// Exact assignment fence carried by the accepted call.
        fence: GroupPositionFence,
    },
    /// The driver rejected the request before transport ownership.
    DriverRejected {
        /// Exact assignment fence carried by the rejected request.
        fence: GroupPositionFence,
        /// Current monotonic terminal observation.
        now: Moment,
    },
    /// The original absolute bootstrap deadline elapsed.
    DeadlineElapsed {
        /// Exact assignment fence whose deadline elapsed.
        fence: GroupPositionFence,
        /// Current monotonic observation proving expiration.
        now: Moment,
    },
    /// Kafka returned one exact group-level rejection.
    BrokerRejected {
        /// Exact assignment fence carried by the response.
        fence: GroupPositionFence,
        /// Current monotonic response observation.
        now: Moment,
        /// Exact nonzero signed Kafka error code.
        error: GroupPositionBrokerError,
    },
    /// Kafka returned ordered per-partition committed-position facts.
    OffsetsFetched {
        /// Exact assignment fence carried by the response.
        fence: GroupPositionFence,
        /// Current monotonic response observation.
        now: Moment,
        /// Throttle and facts in exact request order.
        batch: GroupPositionBatch,
    },
    /// Driver or protocol execution terminally failed.
    FetchFailed {
        /// Exact assignment fence carried by the failed call.
        fence: GroupPositionFence,
        /// Current monotonic terminal observation.
        now: Moment,
        /// Exact normalized failure category.
        failure: GroupPositionBootstrapFetchFailure,
    },
}

impl GroupPositionBootstrapInput {
    pub(crate) const fn fence(&self) -> GroupPositionFence {
        match self {
            Self::Start { fence, .. }
            | Self::DriverAccepted { fence }
            | Self::DriverRejected { fence, .. }
            | Self::DeadlineElapsed { fence, .. }
            | Self::BrokerRejected { fence, .. }
            | Self::OffsetsFetched { fence, .. }
            | Self::FetchFailed { fence, .. } => *fence,
        }
    }
}

/// Lifecycle stage for one position bootstrap owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapState {
    /// Construction retained request ownership without starting work.
    Ready,
    /// The one `OffsetFetch` effect awaits driver acceptance.
    AwaitingDriver,
    /// The driver owns the sole `OffsetFetch` attempt.
    Submitted,
    /// Core assigned the sole terminal decision.
    Completed,
}

/// Rejected assignment fence or lifecycle fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupPositionBootstrapMachineError {
    /// The supplied fact belongs to another membership or assignment.
    StaleFence,
    /// The fact does not belong to the current lifecycle stage.
    InvalidState,
    /// A deadline fact arrived before the original deadline.
    DeadlineNotElapsed,
    /// Core already assigned the sole terminal decision.
    AlreadyCompleted,
}

/// Lossless rejection of one normalized bootstrap fact.
#[must_use = "rejected group position fact must be recovered or deliberately settled"]
#[derive(Debug, Eq, PartialEq)]
pub struct GroupPositionBootstrapApplyError {
    kind: GroupPositionBootstrapMachineError,
    input: GroupPositionBootstrapInput,
}

impl GroupPositionBootstrapApplyError {
    pub(crate) const fn new(
        kind: GroupPositionBootstrapMachineError,
        input: GroupPositionBootstrapInput,
    ) -> Self {
        Self { kind, input }
    }

    /// Returns the deterministic rejection category.
    pub const fn kind(&self) -> GroupPositionBootstrapMachineError {
        self.kind
    }

    /// Borrows the exact rejected fact.
    pub const fn input(&self) -> &GroupPositionBootstrapInput {
        &self.input
    }

    /// Recovers the exact rejected fact.
    pub fn into_input(self) -> GroupPositionBootstrapInput {
        self.input
    }
}

impl fmt::Display for GroupPositionBootstrapApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "group position bootstrap rejected fact: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupPositionBootstrapApplyError {}
