//! Wire-free KIP-848 consumer-group and member descriptions.

use super::modern_assignment::ConsumerGroupDescribeAssignment;

/// One KIP-848 group member with current and target assignment state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupDescribeMember {
    member_id: String,
    instance_id: Option<String>,
    rack_id: Option<String>,
    member_epoch: i32,
    client_id: String,
    client_host: String,
    subscribed_topic_names: Vec<String>,
    subscribed_topic_regex: Option<String>,
    assignment: ConsumerGroupDescribeAssignment,
    target_assignment: ConsumerGroupDescribeAssignment,
    member_type: Option<i8>,
}

impl ConsumerGroupDescribeMember {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        member_id: String,
        instance_id: Option<String>,
        rack_id: Option<String>,
        member_epoch: i32,
        client_id: String,
        client_host: String,
        subscribed_topic_names: Vec<String>,
        subscribed_topic_regex: Option<String>,
        assignment: ConsumerGroupDescribeAssignment,
        target_assignment: ConsumerGroupDescribeAssignment,
        member_type: Option<i8>,
    ) -> Self {
        Self {
            member_id,
            instance_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            assignment,
            target_assignment,
            member_type,
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
        Option<String>,
        i32,
        String,
        String,
        Vec<String>,
        Option<String>,
        ConsumerGroupDescribeAssignment,
        ConsumerGroupDescribeAssignment,
        Option<i8>,
    ) {
        (
            self.member_id,
            self.instance_id,
            self.rack_id,
            self.member_epoch,
            self.client_id,
            self.client_host,
            self.subscribed_topic_names,
            self.subscribed_topic_regex,
            self.assignment,
            self.target_assignment,
            self.member_type,
        )
    }
}

/// Successful KIP-848 description of one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConsumerGroupDescribeDescription {
    group_state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
    members: Vec<ConsumerGroupDescribeMember>,
    authorized_operations: Option<i32>,
}

impl ConsumerGroupDescribeDescription {
    pub(crate) const fn new(
        group_state: String,
        group_epoch: i32,
        assignment_epoch: i32,
        assignor_name: String,
        members: Vec<ConsumerGroupDescribeMember>,
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

    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        i32,
        i32,
        String,
        Vec<ConsumerGroupDescribeMember>,
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
