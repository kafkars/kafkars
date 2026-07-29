//! Stable engine terminal values for Admin `FenceProducers`.

use core::fmt;

mod translate;

pub(crate) use translate::translate_terminal;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersDeliveryStatus {
    /// No transactional-ID call in the operation reached Kafka.
    NotSent,
    /// At least one transactional-ID call may have reached Kafka.
    PossiblySent,
}

/// Broker-issued identity which fenced the previous producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminFencedProducerEngineIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl AdminFencedProducerEngineIdentity {
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

/// Exact transactional-ID-level broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminFenceProducerEngineBrokerError {
    code: i16,
}

impl AdminFenceProducerEngineBrokerError {
    /// Returns Kafka's exact signed error code.
    pub const fn code(self) -> i16 {
        self.code
    }
}

/// One caller-correlated producer-fencing result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminFenceProducerEngineResult {
    transactional_id: String,
    result: Result<AdminFencedProducerEngineIdentity, AdminFenceProducerEngineBrokerError>,
}

impl AdminFenceProducerEngineResult {
    /// Consumes this result into identity and exact broker outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Result<AdminFencedProducerEngineIdentity, AdminFenceProducerEngineBrokerError>,
    ) {
        (self.transactional_id, self.result)
    }
}

/// Caller-ordered complete result plus maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminFenceProducersEngineBatch {
    throttle_time_ms: u32,
    results: Vec<AdminFenceProducerEngineResult>,
}

impl AdminFenceProducersEngineBatch {
    /// Consumes the batch into throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<AdminFenceProducerEngineResult>) {
        (self.throttle_time_ms, self.results)
    }
}

/// Stable whole-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersFailureKind {
    /// The original absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the current transactional ID.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded the admitted retained envelope.
    ResponseTooLarge,
    /// The selected broker API cannot represent the operation.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminFenceProducersFailure {
    kind: AdminFenceProducersFailureKind,
    delivery: AdminFenceProducersDeliveryStatus,
}

impl AdminFenceProducersFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminFenceProducersFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> AdminFenceProducersDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersOutcome {
    /// Every requested transactional ID settled in caller order.
    Fenced(AdminFenceProducersEngineBatch),
    /// Execution failed outside an exact transactional-ID broker result.
    Failed(AdminFenceProducersFailure),
}

/// Failure to observe one named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminFenceProducersObserverError {
    /// This linear observer already consumed its terminal.
    AlreadyObserved,
    /// The observer generation is no longer live.
    Stale,
}

impl fmt::Display for AdminFenceProducersObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "Admin FenceProducers result was already observed",
            Self::Stale => "Admin FenceProducers observer is stale",
        })
    }
}

impl std::error::Error for AdminFenceProducersObserverError {}
