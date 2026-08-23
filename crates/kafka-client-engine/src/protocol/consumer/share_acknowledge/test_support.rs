//! Shared core-owned acknowledgement fixture for generated-wire tests.

use core::fmt::Debug;

use kafka_client_core::{
    AssignedTopicPartition, ByteCount, Deadline, GroupId, MemberId, Moment, PartitionIndex,
    ShareAcknowledgeAttempt, ShareAcknowledgement, ShareAcquiredOffsets, ShareAcquiredRange,
    ShareAcquisitionPolicy, ShareDeliveryCount, ShareDisposition, ShareFetchBrokerId,
    ShareFetchSessionEpoch, ShareFetchSessionFence, ShareFetchSessionMachine,
    ShareGroupMemberEpoch, ShareRecordDecision, ShareTopicUuid, TopicId,
};

pub(super) fn prepared_acknowledgement() -> (ShareAcknowledgeAttempt, ShareAcknowledgement) {
    let assignment = vec![partition(1, 0), partition(1, 1), partition(2, 0)];
    let policy = okay(ShareAcquisitionPolicy::try_new(3, 5, ByteCount::new(24)));
    let mut machine = okay(ShareFetchSessionMachine::try_open(
        fence(),
        assignment,
        policy,
    ));
    let fetch = okay(machine.prepare_fetch(Deadline::from_tick(20), Moment::from_tick(1)));
    okay(machine.settle_acquired(
        fetch,
        Moment::from_tick(2),
        vec![
            range(1, 1, 0, 0, 2),
            range(1, 1, 1, 4, 4),
            range(2, 2, 0, 6, 6),
        ],
    ));
    let acquisitions = okay(machine.ledger_mut().claim_batch(
        fetch.fence(),
        3,
        Moment::from_tick(2),
    ));
    let decisions = vec![
        ShareRecordDecision::new(acquisitions[0].generation(), 0, ShareDisposition::Accept),
        ShareRecordDecision::new(acquisitions[0].generation(), 1, ShareDisposition::Release),
        ShareRecordDecision::new(acquisitions[0].generation(), 2, ShareDisposition::Reject),
        ShareRecordDecision::new(acquisitions[1].generation(), 4, ShareDisposition::Accept),
        ShareRecordDecision::new(acquisitions[2].generation(), 6, ShareDisposition::Reject),
    ];
    let acknowledgement = okay(ShareAcknowledgement::try_new(acquisitions, decisions));
    let admission = okay(machine.prepare_acknowledgement(
        acknowledgement,
        Deadline::from_tick(30),
        Moment::from_tick(3),
    ));
    admission.into_parts()
}

fn range(uuid: u8, topic: u64, partition_index: u32, first: i64, last: i64) -> ShareAcquiredRange {
    let mut topic_uuid = [0; 16];
    topic_uuid[0] = uuid;
    okay(ShareAcquiredRange::try_new(
        some(ShareTopicUuid::try_from_bytes(topic_uuid)),
        partition(topic, partition_index),
        okay(ShareAcquiredOffsets::try_new(first, last)),
        some(ShareDeliveryCount::try_from_raw(1)),
        ByteCount::new(8),
        Deadline::from_tick(50),
        Moment::from_tick(0),
    ))
}

const fn partition(topic: u64, partition_index: u32) -> AssignedTopicPartition {
    AssignedTopicPartition::new(
        TopicId::from_raw(topic),
        PartitionIndex::from_raw(partition_index),
    )
}

fn fence() -> ShareFetchSessionFence {
    ShareFetchSessionFence::new(
        some(ShareFetchBrokerId::try_from_raw(1)),
        some(GroupId::try_from_raw(1)),
        some(MemberId::try_from_raw(1)),
        some(ShareGroupMemberEpoch::try_from_raw(1)),
        ShareFetchSessionEpoch::initial(),
    )
}

fn okay<T, E: Debug>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("unexpected fixture error: {error:?}"))
}

fn some<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("expected validated fixture value"))
}

pub(super) fn id(value: u8) -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = value;
    id
}
