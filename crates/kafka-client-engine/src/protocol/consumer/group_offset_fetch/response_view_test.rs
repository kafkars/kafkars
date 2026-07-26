//! Common generated response-view extraction scenarios.

use kafka_wire::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
};

use super::{model::GroupOffsetFetchPartitionValueRef, response_view::partition_value};

#[test]
fn both_generated_partition_shapes_preserve_nullable_metadata_and_sentinels() {
    let mut legacy = OffsetFetchResponsePartition::default();
    legacy.committed_offset = -1;
    legacy.committed_leader_epoch = -1;
    legacy.metadata = None;
    assert_eq!(
        partition_value(&legacy, 7),
        GroupOffsetFetchPartitionValueRef::Fetched {
            committed_offset: None,
            committed_leader_epoch: None,
            metadata: None,
        }
    );

    let mut modern = OffsetFetchResponsePartitions::default();
    modern.committed_offset = 17;
    modern.committed_leader_epoch = 4;
    modern.metadata = Some("checkpoint".into());
    assert_eq!(
        partition_value(&modern, 9),
        GroupOffsetFetchPartitionValueRef::Fetched {
            committed_offset: Some(17),
            committed_leader_epoch: Some(4),
            metadata: Some("checkpoint"),
        }
    );
}
