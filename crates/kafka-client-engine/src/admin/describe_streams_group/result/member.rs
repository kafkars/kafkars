//! Stable API-89 streams member result values.

use super::{
    DescribeStreamsGroupAssignment, DescribeStreamsGroupEndpoint, DescribeStreamsGroupKeyValue,
    DescribeStreamsGroupTaskOffset,
};

/// One stable streams-group member description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeStreamsGroupMember {
    pub(super) member_id: String,
    pub(super) member_epoch: i32,
    pub(super) instance_id: Option<String>,
    pub(super) rack_id: Option<String>,
    pub(super) client_id: String,
    pub(super) client_host: String,
    pub(super) topology_epoch: i32,
    pub(super) process_id: String,
    pub(super) user_endpoint: Option<DescribeStreamsGroupEndpoint>,
    pub(super) client_tags: Vec<DescribeStreamsGroupKeyValue>,
    pub(super) task_offsets: Vec<DescribeStreamsGroupTaskOffset>,
    pub(super) task_end_offsets: Vec<DescribeStreamsGroupTaskOffset>,
    pub(super) assignment: DescribeStreamsGroupAssignment,
    pub(super) target_assignment: DescribeStreamsGroupAssignment,
    pub(super) is_classic: bool,
}

impl DescribeStreamsGroupMember {
    /// Creates one exact member description.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        member_id: String,
        member_epoch: i32,
        instance_id: Option<String>,
        rack_id: Option<String>,
        client_id: String,
        client_host: String,
        topology_epoch: i32,
        process_id: String,
        user_endpoint: Option<DescribeStreamsGroupEndpoint>,
        client_tags: Vec<DescribeStreamsGroupKeyValue>,
        task_offsets: Vec<DescribeStreamsGroupTaskOffset>,
        task_end_offsets: Vec<DescribeStreamsGroupTaskOffset>,
        assignment: DescribeStreamsGroupAssignment,
        target_assignment: DescribeStreamsGroupAssignment,
        is_classic: bool,
    ) -> Self {
        Self {
            member_id,
            member_epoch,
            instance_id,
            rack_id,
            client_id,
            client_host,
            topology_epoch,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            assignment,
            target_assignment,
            is_classic,
        }
    }

    /// Consumes this member into exact parts.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        String,
        i32,
        Option<String>,
        Option<String>,
        String,
        String,
        i32,
        String,
        Option<DescribeStreamsGroupEndpoint>,
        Vec<DescribeStreamsGroupKeyValue>,
        Vec<DescribeStreamsGroupTaskOffset>,
        Vec<DescribeStreamsGroupTaskOffset>,
        DescribeStreamsGroupAssignment,
        DescribeStreamsGroupAssignment,
        bool,
    ) {
        (
            self.member_id,
            self.member_epoch,
            self.instance_id,
            self.rack_id,
            self.client_id,
            self.client_host,
            self.topology_epoch,
            self.process_id,
            self.user_endpoint,
            self.client_tags,
            self.task_offsets,
            self.task_end_offsets,
            self.assignment,
            self.target_assignment,
            self.is_classic,
        )
    }
}
