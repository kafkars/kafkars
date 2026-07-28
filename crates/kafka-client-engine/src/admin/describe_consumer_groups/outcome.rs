//! Stable engine terminal values for consumer-group description.

mod translate;

use core::fmt;

pub(crate) use translate::translate_terminal;

use super::ConsumerGroupDescription;

/// Stable delivery certainty independent of core and driver types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConsumerGroupsDeliveryStatus {
    /// No request for this group reached the driver.
    NotSent,
    /// One or more requests may have reached a broker.
    PossiblySent,
}

/// Exact per-group broker rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupBrokerError {
    pub(super) code: i16,
    pub(super) message: Option<String>,
    pub(super) message_truncated: bool,
}

impl ConsumerGroupBrokerError {
    /// Consumes this error into stable diagnostic parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }
}

/// Exact failure for one caller-ordered group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConsumerGroupDescriptionError {
    /// Kafka rejected this group with an exact signed broker code.
    Broker(ConsumerGroupBrokerError),
    /// The request mechanism failed for this group.
    Operation(DescribeConsumerGroupsFailure),
}

/// Exact result for one caller-ordered group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerGroupDescriptionResult {
    pub(super) group_id: String,
    pub(super) result: Result<ConsumerGroupDescription, ConsumerGroupDescriptionError>,
}

impl ConsumerGroupDescriptionResult {
    /// Consumes identity and exact group outcome.
    pub fn into_parts(
        self,
    ) -> (
        String,
        Result<ConsumerGroupDescription, ConsumerGroupDescriptionError>,
    ) {
        (self.group_id, self.result)
    }
}

/// Successful operation terminal with maximum observed throttle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeConsumerGroupsBatch {
    pub(super) throttle_time_ms: u32,
    pub(super) groups: Vec<ConsumerGroupDescriptionResult>,
}

impl DescribeConsumerGroupsBatch {
    /// Consumes throttle and caller-ordered results.
    pub fn into_parts(self) -> (u32, Vec<ConsumerGroupDescriptionResult>) {
        (self.throttle_time_ms, self.groups)
    }
}

/// Stable accepted-operation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConsumerGroupsFailureKind {
    /// The public absolute deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a call before accepting it.
    DriverRejected,
    /// Transport failed after call submission.
    Transport,
    /// A response exceeded the operation's retained-byte envelope.
    ResponseTooLarge,
    /// No compatible group-description version was available.
    Compatibility,
    /// The broker response violated the expected singleton response shape.
    InvalidResponse,
    /// This group was not attempted because an earlier group failed.
    NotAttempted,
}

/// Accepted-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeConsumerGroupsFailure {
    pub(super) kind: DescribeConsumerGroupsFailureKind,
    pub(super) delivery: DescribeConsumerGroupsDeliveryStatus,
}

impl DescribeConsumerGroupsFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> DescribeConsumerGroupsFailureKind {
        self.kind
    }

    /// Returns authoritative delivery certainty.
    pub const fn delivery(self) -> DescribeConsumerGroupsDeliveryStatus {
        self.delivery
    }
}

/// Exactly one engine-owned terminal decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeConsumerGroupsOutcome {
    /// Per-group outcomes in the caller's original order.
    Groups(DescribeConsumerGroupsBatch),
    /// A legacy whole-operation terminal retained for exhaustive translation.
    Failed(DescribeConsumerGroupsFailure),
}

/// Failure to observe a named completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeConsumerGroupsObserverError {
    /// The single terminal value was already observed.
    AlreadyObserved,
    /// The observer no longer identifies a live completion slot.
    Stale,
}

impl fmt::Display for DescribeConsumerGroupsObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "DescribeConsumerGroups result was already observed",
            Self::Stale => "DescribeConsumerGroups observer is stale",
        })
    }
}

impl std::error::Error for DescribeConsumerGroupsObserverError {}
