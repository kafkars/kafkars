//! Stable generated-free results for Admin `DescribeShareGroup`.

/// One assigned topic and its deterministic partition set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupTopicAssignment {
    pub(super) topic_id: [u8; 16],
    pub(super) topic_name: String,
    pub(super) partitions: Vec<i32>,
}

impl DescribeShareGroupTopicAssignment {
    /// Consumes this topic into exact stable parts.
    pub fn into_parts(self) -> ([u8; 16], String, Vec<i32>) {
        (self.topic_id, self.topic_name, self.partitions)
    }
}

/// One member's current assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupAssignment {
    pub(super) topics: Vec<DescribeShareGroupTopicAssignment>,
}

impl DescribeShareGroupAssignment {
    /// Consumes this assignment into deterministic topic assignments.
    pub fn into_topics(self) -> Vec<DescribeShareGroupTopicAssignment> {
        self.topics
    }
}

/// One stable share-group member description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupMember {
    pub(super) member_id: String,
    pub(super) rack_id: Option<String>,
    pub(super) member_epoch: i32,
    pub(super) client_id: String,
    pub(super) client_host: String,
    pub(super) subscribed_topic_names: Vec<String>,
    pub(super) assignment: DescribeShareGroupAssignment,
}

impl DescribeShareGroupMember {
    /// Consumes this member into exact stable parts.
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

/// Successful description of one exact share group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupDescription {
    pub(super) group_id: String,
    pub(super) state: String,
    pub(super) group_epoch: i32,
    pub(super) assignment_epoch: i32,
    pub(super) assignor_name: String,
    pub(super) members: Vec<DescribeShareGroupMember>,
    pub(super) authorized_operations: Option<i32>,
}

impl DescribeShareGroupDescription {
    /// Returns the exact requested share-group identity.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Consumes this description into exact stable parts.
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

/// Successful API-77 result plus Kafka's throttle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeShareGroupResult {
    pub(super) throttle_time_ms: u32,
    pub(super) description: DescribeShareGroupDescription,
}

impl DescribeShareGroupResult {
    /// Returns Kafka's nonnegative throttle observation.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns the exact share-group description.
    pub const fn description(&self) -> &DescribeShareGroupDescription {
        &self.description
    }

    /// Consumes this result into exact stable parts.
    pub fn into_parts(self) -> (u32, DescribeShareGroupDescription) {
        (self.throttle_time_ms, self.description)
    }
}
