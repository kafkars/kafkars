//! Exact broker failures and concrete bindings matched by ACL deletion filters.

use core::{cmp::Ordering, num::NonZeroI16};

const UNASSIGNED_RESPONSE_INDEX: usize = usize::MAX;

/// Exact broker-declared failure with a bounded nullable diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteAclBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DeleteAclBrokerError {
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

    /// Consumes this error into adapter-owned exact parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact Kafka result for one binding matching a deletion filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteAclMatchResult {
    /// Kafka deleted this exact binding.
    Deleted,
    /// Kafka rejected deletion of this binding.
    BrokerFailed(DeleteAclBrokerError),
}

/// One exact concrete binding returned under a positional filter result.
#[derive(Clone, Debug)]
pub struct DeleteAclMatchingBinding {
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
    result: DeleteAclMatchResult,
    response_index: usize,
}

impl DeleteAclMatchingBinding {
    /// Creates one protocol-normalized matching binding for core validation.
    pub const fn new(
        resource_type: i8,
        resource_name: String,
        pattern_type: i8,
        principal: String,
        host: String,
        operation: i8,
        permission_type: i8,
        result: DeleteAclMatchResult,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission_type,
            result,
            response_index: UNASSIGNED_RESPONSE_INDEX,
        }
    }

    /// Returns Kafka's exact concrete resource type.
    pub const fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the exact nonempty resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns Kafka's exact concrete pattern type.
    pub const fn pattern_type(&self) -> i8 {
        self.pattern_type
    }

    /// Returns the exact nonempty principal.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the exact nonempty host or explicit wildcard.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns Kafka's exact concrete operation.
    pub const fn operation(&self) -> i8 {
        self.operation
    }

    /// Returns Kafka's exact concrete permission type.
    pub const fn permission_type(&self) -> i8 {
        self.permission_type
    }

    /// Returns the exact deletion result for this matching binding.
    pub const fn result(&self) -> &DeleteAclMatchResult {
        &self.result
    }

    /// Consumes this binding into adapter-owned exact parts.
    pub fn into_parts(self) -> (i8, String, i8, String, String, i8, i8, DeleteAclMatchResult) {
        (
            self.resource_type,
            self.resource_name,
            self.pattern_type,
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
            self.result,
        )
    }

    pub(crate) fn assign_response_index(&mut self, index: usize) {
        self.response_index = index;
    }

    pub(crate) const fn response_index(&self) -> usize {
        self.response_index
    }

    pub(crate) fn clear_response_index(&mut self) {
        self.response_index = UNASSIGNED_RESPONSE_INDEX;
    }

    pub(crate) fn identity_cmp(&self, other: &Self) -> Ordering {
        self.resource_name
            .as_bytes()
            .cmp(other.resource_name.as_bytes())
            .then_with(|| self.resource_type.cmp(&other.resource_type))
            .then_with(|| self.pattern_type.cmp(&other.pattern_type))
            .then_with(|| self.principal.as_bytes().cmp(other.principal.as_bytes()))
            .then_with(|| self.host.as_bytes().cmp(other.host.as_bytes()))
            .then_with(|| self.operation.cmp(&other.operation))
            .then_with(|| self.permission_type.cmp(&other.permission_type))
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.identity_cmp(other) == Ordering::Equal
    }
}

impl PartialEq for DeleteAclMatchingBinding {
    fn eq(&self, other: &Self) -> bool {
        self.same_identity(other) && self.result == other.result
    }
}

impl Eq for DeleteAclMatchingBinding {}
