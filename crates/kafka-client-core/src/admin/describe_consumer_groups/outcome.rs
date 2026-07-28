//! Protocol-normalized terminal facts for consumer-group description.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

use super::AdminConsumerGroupDescription;

/// Exact broker rejection for one requested group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl AdminConsumerGroupBrokerError {
    /// Creates one exact signed Kafka group error.
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

    /// Returns Kafka's nullable diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Returns whether the diagnostic was truncated at the bounded seam.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this error into adapter-owned scalar parts.
    pub fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result for one requested group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminConsumerGroupDescriptionResult {
    /// Kafka described the group.
    Described(AdminConsumerGroupDescription),
    /// Kafka rejected this group with an exact signed code.
    BrokerFailed(AdminConsumerGroupBrokerError),
    /// This group could not complete because the operation mechanism failed.
    OperationFailed(AdminDescribeConsumerGroupsFailure),
}

/// One result retained with its caller-order identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminConsumerGroupDescriptionOutcome {
    group_id: String,
    result: AdminConsumerGroupDescriptionResult,
}

impl AdminConsumerGroupDescriptionOutcome {
    /// Creates one successful group result.
    pub const fn described(group_id: String, description: AdminConsumerGroupDescription) -> Self {
        Self {
            group_id,
            result: AdminConsumerGroupDescriptionResult::Described(description),
        }
    }

    /// Creates one exact per-group broker failure.
    pub const fn broker_failed(group_id: String, error: AdminConsumerGroupBrokerError) -> Self {
        Self {
            group_id,
            result: AdminConsumerGroupDescriptionResult::BrokerFailed(error),
        }
    }

    /// Creates one per-group operation failure.
    pub const fn operation_failed(
        group_id: String,
        failure: AdminDescribeConsumerGroupsFailure,
    ) -> Self {
        Self {
            group_id,
            result: AdminConsumerGroupDescriptionResult::OperationFailed(failure),
        }
    }

    /// Returns the correlated group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Consumes this outcome into adapter-owned parts.
    pub fn into_parts(self) -> (String, AdminConsumerGroupDescriptionResult) {
        (self.group_id, self.result)
    }
}

/// Caller-ordered successful operation terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeConsumerGroupsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<AdminConsumerGroupDescriptionOutcome>,
}

impl AdminDescribeConsumerGroupsBatch {
    /// Creates one batch using the maximum throttle observed across coordinators.
    pub const fn new(
        throttle_time_ms: u32,
        outcomes: Vec<AdminConsumerGroupDescriptionOutcome>,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Consumes the batch into throttle and caller-ordered group results.
    pub fn into_parts(self) -> (u32, Vec<AdminConsumerGroupDescriptionOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure category outside exact per-group broker outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsFailureKind {
    /// The original public deadline elapsed.
    DeadlineElapsed,
    /// The driver rejected a prepared coordinator call.
    DriverRejected,
    /// Driver-owned transport failed.
    Transport,
    /// A response could not fit the admitted retained envelope.
    ResponseTooLarge,
    /// Negotiated protocol semantics were insufficient.
    Compatibility,
    /// A response was malformed or could not be correlated.
    InvalidResponse,
    /// This group was not attempted because an earlier group failed.
    NotAttempted,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdminDescribeConsumerGroupsFailure {
    kind: AdminDescribeConsumerGroupsFailureKind,
    delivery: DeliveryStatus,
}

impl AdminDescribeConsumerGroupsFailure {
    pub(crate) const fn new(
        kind: AdminDescribeConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable failure category.
    pub const fn kind(self) -> AdminDescribeConsumerGroupsFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for `DescribeConsumerGroups`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminDescribeConsumerGroupsTerminal {
    /// Every requested group settled in caller order.
    Described(AdminDescribeConsumerGroupsBatch),
    /// A whole-operation mechanism or structural failure occurred.
    Failed(AdminDescribeConsumerGroupsFailure),
}
