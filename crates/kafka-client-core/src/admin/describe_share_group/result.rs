//! Bounded generated-free API-77 description and terminal values.

use core::num::NonZeroI16;

use crate::DeliveryStatus;

/// Maximum UTF-8 bytes retained for one broker diagnostic prefix.
pub const DESCRIBE_SHARE_GROUP_DIAGNOSTIC_BYTES: usize = 1024;
/// Maximum bytes in one response scalar.
pub const DESCRIBE_SHARE_GROUP_MAX_SCALAR_BYTES: usize = i16::MAX as usize;
/// Maximum members accepted in one description.
pub const DESCRIBE_SHARE_GROUP_MAX_MEMBERS: usize = 16 * 1024;
/// Maximum subscribed topics accepted for one member.
pub const DESCRIBE_SHARE_GROUP_MAX_SUBSCRIPTIONS: usize = 16 * 1024;
/// Maximum assigned topics accepted for one member.
pub const DESCRIBE_SHARE_GROUP_MAX_ASSIGNMENT_TOPICS: usize = 16 * 1024;
/// Maximum assigned partitions accepted for one topic.
pub const DESCRIBE_SHARE_GROUP_MAX_PARTITIONS_PER_TOPIC: usize = 1024 * 1024;
/// Maximum aggregate response text accepted by core.
pub const DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES: usize = 2 * 1024 * 1024;
/// Maximum owned terminal bytes accepted by core.
pub const DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES: usize = 4 * 1024 * 1024;

/// One assigned topic and its canonical partition set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupTopicAssignment {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl DescribeShareGroupTopicAssignment {
    /// Creates one protocol-normalized topic assignment.
    pub const fn new(topic_id: [u8; 16], topic_name: String, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name,
            partitions,
        }
    }

    /// Returns Kafka's exact nonzero topic identity.
    pub const fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns the exact UTF-8 topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns canonical nonnegative partition indices.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    /// Consumes this assignment into adapter-owned parts.
    pub fn into_parts(self) -> ([u8; 16], String, Vec<i32>) {
        (self.topic_id, self.topic_name, self.partitions)
    }
}

/// One member's current share-group assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupAssignment {
    topics: Vec<DescribeShareGroupTopicAssignment>,
}

impl DescribeShareGroupAssignment {
    /// Creates one protocol-normalized assignment.
    pub const fn new(topics: Vec<DescribeShareGroupTopicAssignment>) -> Self {
        Self { topics }
    }

    /// Returns canonical topic assignments.
    pub fn topics(&self) -> &[DescribeShareGroupTopicAssignment] {
        &self.topics
    }

    /// Consumes this assignment into canonical topics.
    pub fn into_topics(self) -> Vec<DescribeShareGroupTopicAssignment> {
        self.topics
    }
}

/// One stable share-group member description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupMember {
    member_id: String,
    rack_id: Option<String>,
    member_epoch: i32,
    client_id: String,
    client_host: String,
    subscribed_topic_names: Vec<String>,
    assignment: DescribeShareGroupAssignment,
}

impl DescribeShareGroupMember {
    /// Creates one protocol-normalized member description.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        member_id: String,
        rack_id: Option<String>,
        member_epoch: i32,
        client_id: String,
        client_host: String,
        subscribed_topic_names: Vec<String>,
        assignment: DescribeShareGroupAssignment,
    ) -> Self {
        Self {
            member_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            assignment,
        }
    }

    /// Returns the exact member identity.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the nullable rack identity.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns Kafka's exact signed member epoch.
    pub const fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the exact client identity.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the exact client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns canonical subscribed topic names.
    pub fn subscribed_topic_names(&self) -> &[String] {
        &self.subscribed_topic_names
    }

    /// Returns the canonical current assignment.
    pub const fn assignment(&self) -> &DescribeShareGroupAssignment {
        &self.assignment
    }

    /// Consumes this member into adapter-owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        Option<String>,
        i32,
        String,
        String,
        Vec<String>,
        DescribeShareGroupAssignment,
    ) {
        (
            self.member_id,
            self.rack_id,
            self.member_epoch,
            self.client_id,
            self.client_host,
            self.subscribed_topic_names,
            self.assignment,
        )
    }
}

/// Successful wire-free description of one exact share group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupDescription {
    group_id: String,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
    members: Vec<DescribeShareGroupMember>,
    authorized_operations: Option<i32>,
}

impl DescribeShareGroupDescription {
    /// Creates one protocol-normalized share-group description.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        group_id: String,
        state: String,
        group_epoch: i32,
        assignment_epoch: i32,
        assignor_name: String,
        members: Vec<DescribeShareGroupMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            group_id,
            state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        }
    }

    /// Returns the exact response group identity.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's group-state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns Kafka's exact signed group epoch.
    pub const fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns Kafka's exact signed assignment epoch.
    pub const fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns the selected assignor name.
    pub fn assignor_name(&self) -> &str {
        &self.assignor_name
    }

    /// Returns members in deterministic member-ID byte order.
    pub fn members(&self) -> &[DescribeShareGroupMember] {
        &self.members
    }

    /// Returns requested authorization bits, excluding Kafka's absence sentinel.
    pub const fn authorized_operations(&self) -> Option<i32> {
        self.authorized_operations
    }

    /// Consumes this description into adapter-owned parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        String,
        i32,
        i32,
        String,
        Vec<DescribeShareGroupMember>,
        Option<i32>,
    ) {
        (
            self.group_id,
            self.state,
            self.group_epoch,
            self.assignment_epoch,
            self.assignor_name,
            self.members,
            self.authorized_operations,
        )
    }
}

/// Successful API-77 response facts plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupResult {
    throttle_time_ms: u32,
    description: DescribeShareGroupDescription,
}

impl DescribeShareGroupResult {
    /// Creates one protocol-normalized exact group result.
    pub const fn new(throttle_time_ms: u32, description: DescribeShareGroupDescription) -> Self {
        Self {
            throttle_time_ms,
            description,
        }
    }

    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the exact share-group description.
    pub const fn description(&self) -> &DescribeShareGroupDescription {
        &self.description
    }

    /// Consumes this result into adapter-owned parts.
    pub fn into_parts(self) -> (u32, DescribeShareGroupDescription) {
        (self.throttle_time_ms, self.description)
    }
}

/// Exact API-77 group rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupBrokerError {
    throttle_time_ms: u32,
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeShareGroupBrokerError {
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

/// Whole-operation failure outside an exact broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeShareGroupFailureKind {
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected the prepared request.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// A valid response exceeded admitted retained capacity.
    ResponseTooLarge,
    /// The broker cannot represent stable API-77 v1 semantics.
    Compatibility,
    /// A broker response was malformed or could not be correlated.
    InvalidResponse,
}

/// Whole-operation failure with authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupFailure {
    kind: DescribeShareGroupFailureKind,
    delivery: DeliveryStatus,
}

impl DescribeShareGroupFailure {
    pub(crate) const fn new(kind: DescribeShareGroupFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    /// Returns the stable mechanism-failure category.
    pub const fn kind(self) -> DescribeShareGroupFailureKind {
        self.kind
    }

    /// Returns authoritative transport delivery certainty.
    pub const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}
