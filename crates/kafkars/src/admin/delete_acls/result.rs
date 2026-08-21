//! Positional filter and nested matched-binding ACL deletion results.

use std::time::Duration;

use super::super::{AclBinding, AclBindingFilter};

/// Exact broker-declared deletion failure with a nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteAclBrokerError {
    pub(crate) const fn new(code: i16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's exact signed error code.
    pub const fn code(&self) -> i16 {
        self.code
    }

    /// Returns Kafka's nullable UTF-8-safe diagnostic prefix.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this failure into exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Exact Kafka result for one concrete binding matched by a filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclMatchResult {
    /// Kafka deleted this exact binding.
    Deleted,
    /// Kafka rejected deletion of this binding.
    BrokerFailed(DeleteAclBrokerError),
}

/// One concrete matched binding and its exact Kafka deletion result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclMatchOutcome {
    binding: AclBinding,
    result: DeleteAclMatchResult,
}

impl DeleteAclMatchOutcome {
    pub(crate) const fn new(binding: AclBinding, result: DeleteAclMatchResult) -> Self {
        Self { binding, result }
    }

    /// Returns the concrete binding Kafka matched.
    pub const fn binding(&self) -> &AclBinding {
        &self.binding
    }

    /// Returns Kafka's exact deletion result for this binding.
    pub const fn result(&self) -> &DeleteAclMatchResult {
        &self.result
    }

    /// Consumes this value into its matched binding and result.
    pub fn into_parts(self) -> (AclBinding, DeleteAclMatchResult) {
        (self.binding, self.result)
    }
}

/// Exact result at one caller filter position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclFilterResult {
    /// Kafka evaluated the filter and returned caller-ordered matching bindings.
    Matched(Vec<DeleteAclMatchOutcome>),
    /// Kafka rejected the filter as a whole.
    BrokerFailed(DeleteAclBrokerError),
}

/// One original caller filter position and its exact corresponding result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclFilterOutcome {
    filter: AclBindingFilter,
    result: DeleteAclFilterResult,
}

impl DeleteAclFilterOutcome {
    pub(crate) const fn new(filter: AclBindingFilter, result: DeleteAclFilterResult) -> Self {
        Self { filter, result }
    }

    /// Returns the original filter at this caller position.
    pub const fn filter(&self) -> &AclBindingFilter {
        &self.filter
    }

    /// Returns the exact result corresponding to this filter position.
    pub const fn result(&self) -> &DeleteAclFilterResult {
        &self.result
    }

    /// Consumes this value into its original filter and result.
    pub fn into_parts(self) -> (AclBindingFilter, DeleteAclFilterResult) {
        (self.filter, self.result)
    }
}

/// Fully settled positional filter results with nested matched bindings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclsResult {
    throttle_time: Duration,
    outcomes: Vec<DeleteAclFilterOutcome>,
}

impl DeleteAclsResult {
    pub(crate) const fn new(
        throttle_time: Duration,
        outcomes: Vec<DeleteAclFilterOutcome>,
    ) -> Self {
        Self {
            throttle_time,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns results in exact caller filter order, including duplicates.
    pub fn outcomes(&self) -> &[DeleteAclFilterOutcome] {
        &self.outcomes
    }

    /// Consumes this result into exact caller-position outcomes.
    pub fn into_outcomes(self) -> Vec<DeleteAclFilterOutcome> {
        self.outcomes
    }
}
