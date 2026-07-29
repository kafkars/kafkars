//! Wire-free exact values for one normalized API-77 share-group description.

use core::num::NonZeroI16;

/// One assigned topic and its canonical partition set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DescribeShareGroupTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl DescribeShareGroupTopicPartitions {
    pub(crate) const fn new(topic_id: [u8; 16], topic_name: String, partitions: Vec<i32>) -> Self {
        Self {
            topic_id,
            topic_name,
            partitions,
        }
    }

    pub(crate) const fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    pub(crate) fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub(crate) fn into_parts(self) -> ([u8; 16], String, Vec<i32>) {
        (self.topic_id, self.topic_name, self.partitions)
    }
}

/// One canonical assignment ordered by topic identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DescribeShareGroupAssignment {
    topics: Vec<DescribeShareGroupTopicPartitions>,
}

impl DescribeShareGroupAssignment {
    pub(crate) const fn new(topics: Vec<DescribeShareGroupTopicPartitions>) -> Self {
        Self { topics }
    }

    #[cfg(test)]
    pub(crate) fn topics(&self) -> &[DescribeShareGroupTopicPartitions] {
        &self.topics
    }

    pub(crate) fn into_topics(self) -> Vec<DescribeShareGroupTopicPartitions> {
        self.topics
    }
}

/// One exact share-group member normalized into deterministic order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DescribeShareGroupMember {
    member_id: String,
    rack_id: Option<String>,
    member_epoch: i32,
    client_id: String,
    client_host: String,
    subscribed_topic_names: Vec<String>,
    assignment: DescribeShareGroupAssignment,
}

impl DescribeShareGroupMember {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
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

    pub(crate) fn member_id(&self) -> &str {
        &self.member_id
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
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

/// Successful exact-v1 description of one share group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DescribeShareGroupDescription {
    group_state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
    members: Vec<DescribeShareGroupMember>,
    authorized_operations: Option<i32>,
}

impl DescribeShareGroupDescription {
    pub(crate) const fn new(
        group_state: String,
        group_epoch: i32,
        assignment_epoch: i32,
        assignor_name: String,
        members: Vec<DescribeShareGroupMember>,
        authorized_operations: Option<i32>,
    ) -> Self {
        Self {
            group_state,
            group_epoch,
            assignment_epoch,
            assignor_name,
            members,
            authorized_operations,
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        i32,
        i32,
        String,
        Vec<DescribeShareGroupMember>,
        Option<i32>,
    ) {
        (
            self.group_state,
            self.group_epoch,
            self.assignment_epoch,
            self.assignor_name,
            self.members,
            self.authorized_operations,
        )
    }
}

/// Exact signed Kafka group rejection with one bounded diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DescribeShareGroupBrokerError {
    code: NonZeroI16,
    message: Option<String>,
    message_truncated: bool,
}

impl DescribeShareGroupBrokerError {
    pub(crate) const fn new(
        code: NonZeroI16,
        message: Option<String>,
        message_truncated: bool,
    ) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    pub(crate) fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code.get(), self.message, self.message_truncated)
    }
}

/// Exact result for the one coordinator-correlated share group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DescribeShareGroupResult {
    Described(DescribeShareGroupDescription),
    Failed(DescribeShareGroupBrokerError),
}

/// One normalized singleton API-77 response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeShareGroupResponse {
    throttle_time_ms: u32,
    group_id: String,
    result: DescribeShareGroupResult,
    retained_bytes: usize,
}

impl NormalizedDescribeShareGroupResponse {
    pub(crate) const fn new(
        throttle_time_ms: u32,
        group_id: String,
        result: DescribeShareGroupResult,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            group_id,
            result,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, String, DescribeShareGroupResult, usize) {
        (
            self.throttle_time_ms,
            self.group_id,
            self.result,
            self.retained_bytes,
        )
    }
}
