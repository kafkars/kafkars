//! Caller-ordered ACL creation results and terminal facts.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::{CreateAclBinding, CreateAclsPlan};

/// Maximum retained UTF-8 broker diagnostic prefix per binding.
pub const CREATE_ACLS_DIAGNOSTIC_BYTES: usize = 1024;

/// Exact broker-declared failure for one requested ACL binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl CreateAclBrokerError {
    /// Creates one exact signed error with an already-bounded diagnostic.
    pub const fn new(code: NonZeroI16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this failure into adapter-owned exact parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact Kafka result for one requested binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclResult {
    /// Kafka accepted this binding.
    Created,
    /// Kafka rejected this binding with an exact signed error.
    BrokerFailed(CreateAclBrokerError),
}

/// One settled caller-ordered result vector and Kafka throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclsBatch {
    throttle_time_ms: u32,
    bindings: Vec<CreateAclBinding>,
    results: Vec<CreateAclResult>,
}

impl CreateAclsBatch {
    pub(crate) fn from_plan(
        throttle_time_ms: u32,
        plan: CreateAclsPlan,
        results: Vec<CreateAclResult>,
    ) -> Self {
        Self {
            throttle_time_ms,
            bindings: plan.into_bindings(),
            results,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns requested bindings in exact caller order.
    pub fn bindings(&self) -> &[CreateAclBinding] {
        &self.bindings
    }

    /// Returns exact per-binding results in matching caller order.
    pub fn results(&self) -> &[CreateAclResult] {
        &self.results
    }

    /// Iterates binding/result pairs without allocating another outcome vector.
    pub fn outcomes(&self) -> impl ExactSizeIterator<Item = (&CreateAclBinding, &CreateAclResult)> {
        self.bindings.iter().zip(&self.results)
    }

    /// Consumes this batch into its already-reserved caller-ordered vectors.
    pub fn into_parts(self) -> (u32, Vec<CreateAclBinding>, Vec<CreateAclResult>) {
        (self.throttle_time_ms, self.bindings, self.results)
    }
}

/// Whole-operation failure category outside the per-binding Kafka results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAclsFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A structurally valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// Negotiated protocol semantics were insufficient.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateAclsFailure {
    kind: CreateAclsFailureKind,
    delivery: DeliveryStatus,
}

impl CreateAclsFailure {
    pub(crate) const fn new(kind: CreateAclsFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> CreateAclsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `CreateAcls`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclsTerminal {
    /// Every requested binding has a caller-ordered exact result.
    Created(CreateAclsBatch),
    /// The whole operation failed outside a complete per-binding result set.
    Failed(CreateAclsFailure),
}
