//! Bounded generated-free API-89 description and terminal values.

mod group;
mod member;
mod topology_description;
mod value;

use core::num::NonZeroI16;

use crate::DeliveryStatus;

pub use group::{DescribeStreamsGroupDescription, DescribeStreamsGroupResult};
pub use member::DescribeStreamsGroupMember;
pub use topology_description::{
    DescribeStreamsGroupTopologyDescription, DescribeStreamsGroupTopologyDescriptionGlobalStore,
    DescribeStreamsGroupTopologyDescriptionNode, DescribeStreamsGroupTopologyDescriptionStatus,
    DescribeStreamsGroupTopologyDescriptionSubtopology,
};
pub use value::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupEndpoint, DescribeStreamsGroupKeyValue,
    DescribeStreamsGroupSubtopology, DescribeStreamsGroupTaskIds, DescribeStreamsGroupTaskOffset,
    DescribeStreamsGroupTopicInfo, DescribeStreamsGroupTopology,
};

/// Maximum UTF-8 bytes retained for one broker diagnostic prefix.
pub const DESCRIBE_STREAMS_GROUP_DIAGNOSTIC_BYTES: usize = 1024;
/// Maximum bytes in one response scalar.
pub const DESCRIBE_STREAMS_GROUP_MAX_SCALAR_BYTES: usize = i16::MAX as usize;
/// Maximum entries accepted for any one nested collection.
pub const DESCRIBE_STREAMS_GROUP_MAX_COLLECTION_ITEMS: usize = 16 * 1024;
/// Maximum partitions accepted for one task group.
pub const DESCRIBE_STREAMS_GROUP_MAX_PARTITIONS_PER_TASK: usize = 1024 * 1024;
/// Maximum aggregate response text accepted by core.
pub const DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES: usize = 2 * 1024 * 1024;
/// Maximum owned terminal bytes accepted by core.
pub const DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;

/// Exact API-89 group rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeStreamsGroupBrokerError {
    /// Creates one exact signed rejection with an already-bounded diagnostic.
    pub const fn new(
        throttle_time_ms: u32,
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            throttle_time_ms,
            code,
            message,
            message_truncated,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact signed nonzero group error code.
    pub const fn code(&self) -> i16 {
        self.code.get()
    }

    /// Returns Kafka's nullable bounded diagnostic.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Reports whether a present diagnostic was truncated.
    pub const fn message_truncated(&self) -> bool {
        self.message_truncated
    }

    /// Consumes this rejection into exact scalar parts.
    pub fn into_parts(self) -> (u32, i16, Option<String>, bool) {
        (
            self.throttle_time_ms,
            self.code.get(),
            self.message,
            self.message_truncated,
        )
    }
}

/// Exact result for one streams group in a caller-ordered batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupOutcome {
    /// Kafka returned one exact correlated group description.
    Described(DescribeStreamsGroupResult),
    /// Kafka rejected this specific streams group.
    BrokerRejected {
        /// Exact requested streams-group identity.
        group_id: String,
        /// Exact signed rejection, throttle, and bounded diagnostic.
        error: DescribeStreamsGroupBrokerError,
    },
}

impl DescribeStreamsGroupOutcome {
    /// Creates one successful per-group outcome.
    pub const fn described(result: DescribeStreamsGroupResult) -> Self {
        Self::Described(result)
    }

    /// Creates one rejected per-group outcome.
    pub const fn broker_rejected(group_id: String, error: DescribeStreamsGroupBrokerError) -> Self {
        Self::BrokerRejected { group_id, error }
    }

    /// Returns the exact requested streams-group identity.
    pub fn group_id(&self) -> &str {
        match self {
            Self::Described(result) => result.description().group_id(),
            Self::BrokerRejected { group_id, .. } => group_id,
        }
    }

    /// Returns this group's nonnegative broker throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        match self {
            Self::Described(result) => result.throttle_time_ms(),
            Self::BrokerRejected { error, .. } => error.throttle_time_ms(),
        }
    }
}

/// Caller-ordered outcomes for one batch operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupsBatch {
    throttle_time_ms: u32,
    outcomes: Vec<DescribeStreamsGroupOutcome>,
}

impl DescribeStreamsGroupsBatch {
    /// Creates one normalized batch with the maximum observed broker throttle.
    pub const fn new(throttle_time_ms: u32, outcomes: Vec<DescribeStreamsGroupOutcome>) -> Self {
        Self {
            throttle_time_ms,
            outcomes,
        }
    }

    /// Returns the maximum nonnegative throttle observed across coordinator calls.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns exactly one outcome per requested group in caller order.
    pub fn outcomes(&self) -> &[DescribeStreamsGroupOutcome] {
        &self.outcomes
    }

    /// Consumes this batch into adapter-owned parts.
    pub fn into_parts(self) -> (u32, Vec<DescribeStreamsGroupOutcome>) {
        (self.throttle_time_ms, self.outcomes)
    }
}

/// Whole-operation failure outside an exact broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The broker cannot represent the requested API-89 semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupFailure {
    kind: DescribeStreamsGroupFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeStreamsGroupFailure {
    pub(crate) const fn new(
        kind: DescribeStreamsGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> DescribeStreamsGroupFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Exactly one terminal decision for one API-89 operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeStreamsGroupTerminal {
    /// Kafka returned one exact correlated group description.
    Described(DescribeStreamsGroupResult),
    /// Kafka rejected the requested group.
    BrokerRejected(DescribeStreamsGroupBrokerError),
    /// Every requested group settled in original caller order.
    Batch(DescribeStreamsGroupsBatch),
    /// Execution failed outside an exact Kafka rejection.
    Failed(DescribeStreamsGroupFailure),
}
