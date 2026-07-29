//! Terminal verification for concrete consumer and share-group Admin owners.

use super::super::{EngineHostError, EngineHostResources};

pub(super) fn verify(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let group_offsets = resources
        .list_consumer_group_offsets
        .terminal_host()
        .unsettled();
    if group_offsets != 0 {
        return Err(EngineHostError::ListConsumerGroupOffsets(
            crate::admin::ListConsumerGroupOffsetsHostError::Unsettled(group_offsets),
        ));
    }
    let listed_groups = resources.list_consumer_groups.terminal_host().unsettled();
    if listed_groups != 0 {
        return Err(EngineHostError::ListConsumerGroups(
            crate::admin::ListConsumerGroupsHostError::Unsettled(listed_groups),
        ));
    }
    let group_offset_delete = resources
        .delete_consumer_group_offsets
        .terminal_host()
        .unsettled();
    if group_offset_delete != 0 {
        return Err(EngineHostError::DeleteConsumerGroupOffsets(
            crate::admin::DeleteConsumerGroupOffsetsHostError::Unsettled(group_offset_delete),
        ));
    }
    let share_group_offset_delete = resources
        .delete_share_group_offsets
        .terminal_host()
        .unsettled();
    if share_group_offset_delete != 0 {
        return Err(EngineHostError::DeleteShareGroupOffsets(
            crate::admin::delete_share_group_offsets::DeleteShareGroupOffsetsHostError::Unsettled(
                share_group_offset_delete,
            ),
        ));
    }
    let share_group_offset_list = resources
        .list_share_group_offsets
        .terminal_host()
        .unsettled();
    if share_group_offset_list != 0 {
        return Err(EngineHostError::ListShareGroupOffsets(
            crate::admin::list_share_group_offsets::ListShareGroupOffsetsHostError::Unsettled(
                share_group_offset_list,
            ),
        ));
    }
    let share_group_offset_alter = resources
        .alter_share_group_offsets
        .terminal_host()
        .unsettled();
    if share_group_offset_alter != 0 {
        return Err(EngineHostError::AlterShareGroupOffsets(
            crate::admin::alter_share_group_offsets::AlterShareGroupOffsetsHostError::Unsettled(
                share_group_offset_alter,
            ),
        ));
    }
    let share_group_describe = resources.describe_share_group.terminal_host().unsettled();
    if share_group_describe != 0 {
        return Err(EngineHostError::DescribeShareGroup(
            crate::admin::describe_share_group::DescribeShareGroupHostError::Unsettled(
                share_group_describe,
            ),
        ));
    }
    let streams_group_describe = resources.describe_streams_group.terminal_host().unsettled();
    if streams_group_describe != 0 {
        return Err(EngineHostError::DescribeStreamsGroup(
            crate::admin::describe_streams_group::DescribeStreamsGroupHostError::Unsettled(
                streams_group_describe,
            ),
        ));
    }
    let group_offset_alter = resources
        .alter_consumer_group_offsets
        .terminal_host()
        .unsettled();
    if group_offset_alter != 0 {
        return Err(EngineHostError::AlterConsumerGroupOffsets(
            crate::admin::AlterConsumerGroupOffsetsHostError::Unsettled(group_offset_alter),
        ));
    }
    Ok(())
}
