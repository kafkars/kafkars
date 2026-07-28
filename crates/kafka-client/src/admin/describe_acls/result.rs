//! Deterministically ordered ACL bindings with throttle observation.

use std::time::Duration;

use super::super::AclBinding;

/// Fully settled ACL bindings selected by one filter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeAclsResult {
    throttle_time: Duration,
    bindings: Vec<AclBinding>,
}

impl DescribeAclsResult {
    pub(crate) const fn new(throttle_time: Duration, bindings: Vec<AclBinding>) -> Self {
        Self {
            throttle_time,
            bindings,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns bindings in deterministic engine order.
    pub fn bindings(&self) -> &[AclBinding] {
        &self.bindings
    }

    /// Consumes this result into deterministically ordered bindings.
    pub fn into_bindings(self) -> Vec<AclBinding> {
        self.bindings
    }
}
