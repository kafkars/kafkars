//! Caller-ordered exact ACL creation results with throttle observation.

use std::time::Duration;

use super::super::AclBinding;

/// Exact Kafka rejection for one requested ACL binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclBrokerError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl CreateAclBrokerError {
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

    /// Consumes this rejection into its exact diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Exact Kafka result for one requested ACL binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAclResult {
    /// Kafka accepted this binding.
    Created,
    /// Kafka rejected this binding with an exact signed error.
    BrokerFailed(CreateAclBrokerError),
}

/// One requested ACL binding and its exact corresponding Kafka result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclOutcome {
    binding: AclBinding,
    result: CreateAclResult,
}

impl CreateAclOutcome {
    pub(crate) const fn new(binding: AclBinding, result: CreateAclResult) -> Self {
        Self { binding, result }
    }

    /// Returns the requested stable ACL binding.
    pub const fn binding(&self) -> &AclBinding {
        &self.binding
    }

    /// Returns Kafka's exact corresponding result.
    pub const fn result(&self) -> &CreateAclResult {
        &self.result
    }

    /// Consumes this outcome into its binding and result.
    pub fn into_parts(self) -> (AclBinding, CreateAclResult) {
        (self.binding, self.result)
    }
}

/// Fully settled caller-ordered ACL creation results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateAclsResult {
    throttle_time: Duration,
    outcomes: Vec<CreateAclOutcome>,
}

impl CreateAclsResult {
    pub(crate) const fn new(throttle_time: Duration, outcomes: Vec<CreateAclOutcome>) -> Self {
        Self {
            throttle_time,
            outcomes,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns binding results in exact original request order.
    pub fn outcomes(&self) -> &[CreateAclOutcome] {
        &self.outcomes
    }

    /// Consumes this result into exact original-order binding outcomes.
    pub fn into_outcomes(self) -> Vec<CreateAclOutcome> {
        self.outcomes
    }
}
