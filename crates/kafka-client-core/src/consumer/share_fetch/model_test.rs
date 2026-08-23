//! Executable validation evidence for `ShareFetch` scalar and range facts.

use core::fmt::Debug;

use crate::{AssignedTopicPartition, ByteCount, Deadline, Moment, PartitionIndex, TopicId};

use super::{
    ShareAcquiredOffsets, ShareAcquiredRange, ShareAcquiredRangeError, ShareDeliveryCount,
    ShareFetchBrokerId, ShareFetchSessionEpoch, ShareTopicUuid,
};

#[test]
fn scalar_domains_reject_kafka_sentinels() {
    assert_eq!(ShareFetchBrokerId::try_from_raw(-1), None);
    assert_eq!(
        ShareFetchBrokerId::try_from_raw(0).map(ShareFetchBrokerId::get),
        Some(0)
    );
    assert_eq!(ShareFetchSessionEpoch::try_from_raw(-1), None);
    assert_eq!(ShareFetchSessionEpoch::initial().get(), 0);
    assert_eq!(ShareDeliveryCount::try_from_raw(0), None);
    assert_eq!(
        ShareDeliveryCount::try_from_raw(2).map(ShareDeliveryCount::get),
        Some(2)
    );
    assert_eq!(ShareTopicUuid::try_from_bytes([0; 16]), None);
    assert_eq!(topic_uuid(7).bytes()[0], 7);
}

#[test]
fn acquired_ranges_validate_offsets_and_lock_boundary() {
    let partition = AssignedTopicPartition::new(TopicId::from_raw(3), PartitionIndex::from_raw(1));
    assert_eq!(
        ShareAcquiredOffsets::try_new(-1, 4),
        Err(ShareAcquiredRangeError::InvalidOffsets)
    );
    assert_eq!(
        ShareAcquiredOffsets::try_new(4, 3),
        Err(ShareAcquiredRangeError::InvalidOffsets)
    );
    let count = some(ShareDeliveryCount::try_from_raw(1));
    let offsets = okay(ShareAcquiredOffsets::try_new(3, 4));
    assert_eq!(
        ShareAcquiredRange::try_new(
            topic_uuid(1),
            partition,
            offsets,
            count,
            ByteCount::new(12),
            Deadline::from_tick(10),
            Moment::from_tick(10),
        ),
        Err(ShareAcquiredRangeError::ExpiredLock)
    );

    let range = okay(ShareAcquiredRange::try_new(
        topic_uuid(1),
        partition,
        offsets,
        count,
        ByteCount::new(12),
        Deadline::from_tick(11),
        Moment::from_tick(10),
    ));
    assert_eq!(range.record_count(), 2);
    assert_eq!(range.partition(), partition);
    assert_eq!(range.retained_bytes(), ByteCount::new(12));
}

fn topic_uuid(first: u8) -> ShareTopicUuid {
    let mut bytes = [0; 16];
    bytes[0] = first;
    some(ShareTopicUuid::try_from_bytes(bytes))
}

fn okay<T, E: Debug>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("unexpected error: {error:?}"),
    }
}

fn some<T>(value: Option<T>) -> T {
    match value {
        Some(value) => value,
        None => panic!("expected validated value"),
    }
}
