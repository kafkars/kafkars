//! Admission handoffs for consumer and generic group administration.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::{
    admin_delete_consumer_groups::{AdminDeleteConsumerGroups, DeleteConsumerGroupsAdminRequest},
    admin_describe_classic_groups::AdminDescribeClassicGroups,
    admin_describe_consumer_groups::{
        AdminDescribeConsumerGroups, DescribeConsumerGroupsAdminRequest,
    },
    admin_group_offset_delete_operation::AdminDeleteConsumerGroupOffsets,
    admin_group_offset_delete_request::DeleteConsumerGroupOffsetsAdminRequest,
    admin_group_offsets::{
        AdminAlterConsumerGroupOffsets, AdminListConsumerGroupOffsets,
        AdminListConsumerGroupsOffsets, AlterConsumerGroupOffsetsAdminRequest,
        ListConsumerGroupOffsetsAdminRequest, ListConsumerGroupsOffsetsAdminRequest,
    },
    admin_list_consumer_groups::AdminListConsumerGroups,
    admin_list_groups::{AdminListGroups, ListGroupsAdminRequest},
    admin_remove_consumer_group_members::{
        AdminRemoveConsumerGroupMembers, RemoveConsumerGroupMembersAdminRequest,
    },
};

impl AdminEngine {
    pub(crate) fn submit_delete_consumer_groups(
        &self,
        request: DeleteConsumerGroupsAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteConsumerGroups {
        AdminDeleteConsumerGroups::from_admission(
            self.handle
                .try_delete_consumer_groups(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_list_consumer_groups_offsets(
        &self,
        request: ListConsumerGroupsOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminListConsumerGroupsOffsets {
        AdminListConsumerGroupsOffsets::from_admission(
            self.handle
                .try_list_consumer_groups_offsets(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_consumer_groups(
        &self,
        request: DescribeConsumerGroupsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeConsumerGroups {
        AdminDescribeConsumerGroups::from_admission(
            self.handle
                .try_describe_consumer_groups(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_classic_groups(
        &self,
        request: DescribeConsumerGroupsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeClassicGroups {
        AdminDescribeClassicGroups::from_admission(
            self.handle
                .try_describe_classic_groups(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_list_consumer_group_offsets(
        &self,
        request: ListConsumerGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminListConsumerGroupOffsets {
        AdminListConsumerGroupOffsets::from_admission(
            self.handle
                .try_list_consumer_group_offsets(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_list_consumer_groups(&self, timeout: Duration) -> AdminListConsumerGroups {
        AdminListConsumerGroups::from_admission(self.handle.try_list_consumer_groups(timeout))
    }

    pub(crate) fn submit_list_groups(
        &self,
        request: ListGroupsAdminRequest,
        timeout: Duration,
    ) -> AdminListGroups {
        AdminListGroups::from_admission(self.handle.try_list_groups(request.into_engine(), timeout))
    }

    pub(crate) fn submit_delete_consumer_group_offsets(
        &self,
        request: DeleteConsumerGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteConsumerGroupOffsets {
        AdminDeleteConsumerGroupOffsets::from_admission(
            self.handle
                .try_delete_consumer_group_offsets(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_alter_consumer_group_offsets(
        &self,
        request: AlterConsumerGroupOffsetsAdminRequest,
        timeout: Duration,
    ) -> AdminAlterConsumerGroupOffsets {
        AdminAlterConsumerGroupOffsets::from_admission(
            self.handle
                .try_alter_consumer_group_offsets(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_remove_consumer_group_members(
        &self,
        request: RemoveConsumerGroupMembersAdminRequest,
        timeout: Duration,
    ) -> AdminRemoveConsumerGroupMembers {
        AdminRemoveConsumerGroupMembers::from_admission(
            self.handle
                .try_remove_consumer_group_members(request.into_engine(), timeout),
        )
    }
}
