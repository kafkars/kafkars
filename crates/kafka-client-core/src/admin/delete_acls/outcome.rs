//! Positional filter results, exact matching bindings, and terminal facts.

use crate::DeliveryStatus;

use super::{DeleteAclsFilter, DeleteAclsPlan};

mod matching_binding;

pub use matching_binding::{DeleteAclBrokerError, DeleteAclMatchResult, DeleteAclMatchingBinding};

/// Maximum retained UTF-8 broker diagnostic prefix at every error position.
pub const DELETE_ACLS_DIAGNOSTIC_BYTES: usize = 1024;

/// Maximum total matching bindings accepted across one response.
pub const MAX_DELETE_ACLS_MATCHING_BINDINGS: usize = 1024 * 1024;

/// Exact result at one caller filter position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclFilterResult {
    /// Kafka evaluated the filter and returned zero or more matching bindings.
    Matched(Vec<DeleteAclMatchingBinding>),
    /// Kafka rejected this filter as a whole.
    BrokerFailed(DeleteAclBrokerError),
}

/// One settled positional filter-result vector and throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsBatch {
    throttle_time_ms: u32,
    filters: Vec<DeleteAclsFilter>,
    results: Vec<DeleteAclFilterResult>,
}

impl DeleteAclsBatch {
    pub(crate) fn from_plan(
        throttle_time_ms: u32,
        plan: DeleteAclsPlan,
        results: Vec<DeleteAclFilterResult>,
    ) -> Self {
        Self {
            throttle_time_ms,
            filters: plan.into_filters(),
            results,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns filters in exact caller order, including duplicates.
    pub fn filters(&self) -> &[DeleteAclsFilter] {
        &self.filters
    }

    /// Returns positional results in matching caller order.
    pub fn results(&self) -> &[DeleteAclFilterResult] {
        &self.results
    }

    /// Iterates filter/result positions without allocating another vector.
    pub fn outcomes(
        &self,
    ) -> impl ExactSizeIterator<Item = (&DeleteAclsFilter, &DeleteAclFilterResult)> {
        self.filters.iter().zip(&self.results)
    }

    /// Consumes this batch into its already-reserved positional vectors.
    pub fn into_parts(self) -> (u32, Vec<DeleteAclsFilter>, Vec<DeleteAclFilterResult>) {
        (self.throttle_time_ms, self.filters, self.results)
    }
}

/// Whole-operation failure category outside complete positional results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteAclsFailureKind {
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
pub struct DeleteAclsFailure {
    kind: DeleteAclsFailureKind,
    delivery: DeliveryStatus,
}

impl DeleteAclsFailure {
    pub(crate) const fn new(kind: DeleteAclsFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> DeleteAclsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for Admin `DeleteAcls`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclsTerminal {
    /// Every caller filter position has one exact result.
    Deleted(DeleteAclsBatch),
    /// The whole operation failed outside complete positional results.
    Failed(DeleteAclsFailure),
}
