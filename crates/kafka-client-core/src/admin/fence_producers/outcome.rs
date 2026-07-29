//! Caller-correlated producer-fencing outcomes and terminal facts.

use core::num::NonZeroI16;

use super::AdminFenceProducersFailure;

/// Producer identity returned by a successful fencing `InitProducerId`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminFencedProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl AdminFencedProducerIdentity {
    /// Creates one valid nonnegative Kafka producer identity.
    pub const fn try_new(producer_id: i64, producer_epoch: i16) -> Option<Self> {
        if producer_id < 0 || producer_epoch < 0 {
            None
        } else {
            Some(Self {
                producer_id,
                producer_epoch,
            })
        }
    }

    /// Returns Kafka's assigned producer ID.
    pub const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's assigned producer epoch.
    pub const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }

    /// Consumes the identity into stable scalar parts.
    pub const fn into_parts(self) -> (i64, i16) {
        (self.producer_id, self.producer_epoch)
    }
}

/// Exact per-transactional-ID broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminFenceProducerBrokerError {
    code: NonZeroI16,
}

impl AdminFenceProducerBrokerError {
    /// Creates one exact signed Kafka error.
    pub const fn new(code: NonZeroI16) -> Self {
        Self { code }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code.get()
    }
}

/// Exact result Kafka returned for one requested transactional ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducerResult {
    /// Kafka fenced the old producer and returned the successor identity.
    Fenced(AdminFencedProducerIdentity),
    /// Kafka rejected this transactional ID with an exact signed code.
    BrokerFailed(AdminFenceProducerBrokerError),
}

/// One result retained with its caller-order identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminFenceProducerOutcome {
    transactional_id: String,
    result: AdminFenceProducerResult,
}

impl AdminFenceProducerOutcome {
    /// Creates one successful fencing outcome.
    pub const fn fenced(transactional_id: String, identity: AdminFencedProducerIdentity) -> Self {
        Self {
            transactional_id,
            result: AdminFenceProducerResult::Fenced(identity),
        }
    }

    /// Creates one exact per-ID broker rejection.
    pub const fn broker_failed(
        transactional_id: String,
        error: AdminFenceProducerBrokerError,
    ) -> Self {
        Self {
            transactional_id,
            result: AdminFenceProducerResult::BrokerFailed(error),
        }
    }

    /// Returns the correlated transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns this ID's exact result.
    pub const fn result(&self) -> &AdminFenceProducerResult {
        &self.result
    }

    /// Consumes this outcome into stable adapter-owned parts.
    pub fn into_parts(self) -> (String, AdminFenceProducerResult) {
        (self.transactional_id, self.result)
    }
}

/// Caller-ordered result for every requested transactional ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminFenceProducersBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AdminFenceProducerOutcome>,
}

impl AdminFenceProducersBatch {
    /// Creates one settled batch using the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<AdminFenceProducerOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns per-ID outcomes in exact caller order.
    pub fn outcomes(&self) -> &[AdminFenceProducerOutcome] {
        &self.outcomes
    }

    /// Consumes the batch into stable adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<AdminFenceProducerOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Exactly one terminal decision for Admin `FenceProducers`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersTerminal {
    /// Every requested transactional ID settled in caller order.
    Fenced(AdminFenceProducersBatch),
    /// A whole-operation mechanism failure occurred.
    Failed(AdminFenceProducersFailure),
}
