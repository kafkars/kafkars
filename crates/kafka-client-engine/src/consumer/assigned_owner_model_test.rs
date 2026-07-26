//! Construction-bound scenarios for the assigned-owner model.

use kafka_client_core::ReadIsolation;

use super::{
    assigned_owner_model::{
        AssignedConsumerOwnerBuildError, AssignedConsumerOwnerLimits, fetch_isolation,
        position_isolation,
    },
    assigned_owner_test::{OUTPUT_BYTES, limits},
    assigned_topics::AssignedTopicLimits,
};

#[test]
fn core_read_isolation_maps_exhaustively_at_protocol_boundaries() {
    assert_eq!(
        position_isolation(ReadIsolation::ReadUncommitted),
        crate::protocol::consumer::ListOffsetsIsolation::ReadUncommitted
    );
    assert_eq!(
        position_isolation(ReadIsolation::ReadCommitted),
        crate::protocol::consumer::ListOffsetsIsolation::ReadCommitted
    );
    assert_eq!(
        fetch_isolation(ReadIsolation::ReadUncommitted),
        crate::protocol::fetch::FetchIsolation::ReadUncommitted
    );
    assert_eq!(
        fetch_isolation(ReadIsolation::ReadCommitted),
        crate::protocol::fetch::FetchIsolation::ReadCommitted
    );
}

#[test]
fn effect_capacity_is_checked_two_partitions_plus_one() {
    let limits = limits(7);
    assert_eq!(limits.effect_capacity, 15);
}

#[test]
fn topic_partition_bound_cannot_exceed_owner_bound() {
    assert_eq!(
        AssignedConsumerOwnerLimits::new(
            1,
            1,
            1,
            OUTPUT_BYTES,
            OUTPUT_BYTES,
            AssignedTopicLimits::new(2, 2, 249, 4_096),
        ),
        Err(AssignedConsumerOwnerBuildError::TopicPartitionCapacity { topic: 2, owner: 1 })
    );
}

#[test]
fn zero_and_impossible_delivery_bounds_are_rejected() {
    assert!(matches!(
        AssignedConsumerOwnerLimits::new(
            0,
            1,
            1,
            OUTPUT_BYTES,
            OUTPUT_BYTES,
            AssignedTopicLimits::new(1, 0, 249, 4_096),
        ),
        Err(AssignedConsumerOwnerBuildError::ZeroPartitionCapacity)
    ));
    assert!(matches!(
        AssignedConsumerOwnerLimits::new(
            1,
            1,
            1,
            OUTPUT_BYTES,
            OUTPUT_BYTES + 1,
            AssignedTopicLimits::new(1, 1, 249, 4_096),
        ),
        Err(AssignedConsumerOwnerBuildError::FetchOutputBytes { .. })
    ));
}
