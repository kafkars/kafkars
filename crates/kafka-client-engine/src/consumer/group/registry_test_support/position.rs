//! Pure completed-position construction for group-registry tests.

use kafka_client_core::{
    GroupAssignmentPartition, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact,
    Moment, NextFetchOffset,
};

use super::super::classic_group_position::{
    ClassicGroupPositionCompleted, test_support::completed_ready,
};

pub(super) fn completed_committed_position(
    fence: GroupPositionFence,
    partition: GroupAssignmentPartition,
    next_offset: i64,
) -> ClassicGroupPositionCompleted {
    completed_ready(
        fence,
        Moment::from_tick(41),
        GroupPositionBatch::new(
            0,
            vec![GroupPositionPartitionFact::committed(
                partition,
                NextFetchOffset::try_from_raw(next_offset)
                    .unwrap_or_else(|| panic!("next Fetch offset")),
            )],
        ),
    )
}
