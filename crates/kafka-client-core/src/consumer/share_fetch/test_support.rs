//! Shared deterministic constructors for `ShareFetch` unit scenarios.

use core::fmt::Debug;

use crate::{
    AssignedTopicPartition, ByteCount, Deadline, GroupId, MemberId, Moment, PartitionIndex,
    ShareGroupMemberEpoch, TopicId,
};

use super::{
    ShareAcquiredOffsets, ShareAcquiredRange, ShareAcquisitionLedger, ShareAcquisitionPolicy,
    ShareDeliveryCount, ShareFetchBrokerId, ShareFetchSessionEpoch, ShareFetchSessionFence,
    ShareTopicUuid,
};

pub(super) fn ledger(ranges: usize, records: u64, bytes: u64) -> ShareAcquisitionLedger {
    let policy = okay(ShareAcquisitionPolicy::try_new(
        ranges,
        records,
        ByteCount::new(bytes),
    ));
    okay(ShareAcquisitionLedger::try_new(policy))
}

pub(super) fn range(
    uuid: u8,
    topic: u64,
    first: i64,
    last: i64,
    count: i16,
    bytes: u64,
    lock: u64,
) -> ShareAcquiredRange {
    let mut topic_uuid = [0; 16];
    topic_uuid[0] = uuid;
    okay(ShareAcquiredRange::try_new(
        some(ShareTopicUuid::try_from_bytes(topic_uuid)),
        AssignedTopicPartition::new(TopicId::from_raw(topic), PartitionIndex::from_raw(0)),
        okay(ShareAcquiredOffsets::try_new(first, last)),
        some(ShareDeliveryCount::try_from_raw(count)),
        ByteCount::new(bytes),
        Deadline::from_tick(lock),
        Moment::from_tick(0),
    ))
}

pub(super) fn fence(session: i32) -> ShareFetchSessionFence {
    ShareFetchSessionFence::new(
        some(ShareFetchBrokerId::try_from_raw(1)),
        some(GroupId::try_from_raw(1)),
        some(MemberId::try_from_raw(1)),
        some(ShareGroupMemberEpoch::try_from_raw(1)),
        some(ShareFetchSessionEpoch::try_from_raw(session)),
    )
}

pub(super) fn okay<T, E: Debug>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("unexpected error: {error:?}"),
    }
}

pub(super) fn rejected<T, E: Debug>(result: Result<T, E>) -> E {
    match result {
        Ok(_value) => panic!("expected rejection"),
        Err(error) => error,
    }
}

pub(super) fn some<T>(value: Option<T>) -> T {
    match value {
        Some(value) => value,
        None => panic!("expected validated value"),
    }
}
