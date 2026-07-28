//! Generated-free response facts safe for deterministic member-removal policy.

use core::num::NonZeroI16;

use kafka_client_core::RemoveConsumerGroupMembersBatch;

/// Validated `LeaveGroup` response facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValidatedRemoveConsumerGroupMembersResponse {
    /// Exact top-level broker rejection.
    BrokerRejected(NonZeroI16),
    /// Caller-ordered per-member outcomes and throttle.
    Batch(RemoveConsumerGroupMembersBatch),
}
