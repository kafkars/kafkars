//! Linear one-partition Fetch outcomes and local rejection ownership.

use core::num::NonZeroI16;

use super::{
    FetchBatch, FetchResponseFailure,
    retention::{FetchOutputReservation, FetchRetainedCharge, FetchRetentionFailure},
};

/// Protocol level that supplied one exact nonzero broker code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchBrokerLevel {
    TopLevel,
    Partition,
}

/// One broker-owned Fetch failure requiring no application-data retention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchBrokerFailure {
    level: FetchBrokerLevel,
    code: NonZeroI16,
}

impl FetchBrokerFailure {
    pub(super) const fn new(level: FetchBrokerLevel, code: NonZeroI16) -> Self {
        Self { level, code }
    }

    pub(crate) const fn level(self) -> FetchBrokerLevel {
        self.level
    }

    pub(crate) const fn code(self) -> NonZeroI16 {
        self.code
    }
}

/// Mutually exclusive broker failure or successful retained Fetch data.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FetchOutcome {
    BrokerFailure(FetchBrokerFailure),
    Success {
        next_offset: i64,
        data_batches: Box<[FetchBatch]>,
    },
}

impl FetchOutcome {
    pub(crate) const fn broker_failure(&self) -> Option<FetchBrokerFailure> {
        match self {
            Self::BrokerFailure(failure) => Some(*failure),
            Self::Success { .. } => None,
        }
    }

    pub(crate) const fn next_offset(&self) -> Option<i64> {
        match self {
            Self::BrokerFailure(_) => None,
            Self::Success { next_offset, .. } => Some(*next_offset),
        }
    }

    pub(crate) fn data_batches(&self) -> Option<&[FetchBatch]> {
        match self {
            Self::BrokerFailure(_) => None,
            Self::Success { data_batches, .. } => Some(data_batches),
        }
    }
}

/// One terminal Fetch result and its stable retained-output charge.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RetainedFetchOutcome {
    throttle_ticks: Option<u64>,
    outcome: FetchOutcome,
    charge: FetchRetainedCharge,
}

impl RetainedFetchOutcome {
    pub(super) const fn new(
        throttle_ticks: Option<u64>,
        outcome: FetchOutcome,
        charge: FetchRetainedCharge,
    ) -> Self {
        Self {
            throttle_ticks,
            outcome,
            charge,
        }
    }

    pub(crate) const fn throttle_ticks(&self) -> Option<u64> {
        self.throttle_ticks
    }

    pub(crate) const fn outcome(&self) -> &FetchOutcome {
        &self.outcome
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.charge.retained_bytes()
    }

    pub(crate) const fn unused_reserved_bytes(&self) -> usize {
        self.charge.unused_bytes()
    }
}

/// Why raw generated response ownership could not become an engine outcome.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FetchOutcomeFailure {
    InvalidRequestedOffset { actual: i64 },
    NegativeThrottleTime { actual: i32 },
    Response(FetchResponseFailure),
    UnexpectedSessionId { actual: i32 },
    ThrottleTickOverflow { milliseconds: u32 },
    Retention(FetchRetentionFailure),
    CorrelatedShapeLost,
}

/// Failed normalization returning the hard capacity token intact.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RejectedFetchOutcome {
    failure: FetchOutcomeFailure,
    reservation: FetchOutputReservation,
}

impl RejectedFetchOutcome {
    pub(crate) const fn failure(&self) -> &FetchOutcomeFailure {
        &self.failure
    }

    pub(crate) fn into_parts(self) -> (FetchOutcomeFailure, FetchOutputReservation) {
        (self.failure, self.reservation)
    }
}

pub(super) fn reject(
    failure: FetchOutcomeFailure,
    reservation: FetchOutputReservation,
) -> RejectedFetchOutcome {
    RejectedFetchOutcome {
        failure,
        reservation,
    }
}
