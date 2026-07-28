//! Generated name-based v4 `TxnOffsetCommit` construction.
//!
//! Version 4 carries leader epochs, nullable metadata, and classic/consumer
//! group identity scalars while preserving the explicit preceding
//! `AddOffsetsToTxn` step. Version 5 may fuse that step under transaction v2,
//! and version 6 replaces topic names with IDs, so neither is selected here.

use kafka_wire::{
    TxnOffsetCommitRequest,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};

use super::{TransactionGroupIdentityRef, TransactionOffsetCommitRef};

/// Request facts that cannot safely enter the generated v4 shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TxnOffsetCommitRequestFailure {
    EmptyTransactionalId,
    InvalidProducerId { actual: i64 },
    InvalidProducerEpoch { actual: i16 },
    EmptyGroupId,
    NegativeGroupEpoch { actual: i32 },
    EmptyMemberId,
    EmptyGroupInstanceId,
    EmptyOffsets,
    EmptyTopic,
    NegativePartition { actual: i32 },
    NegativeNextOffset { actual: i64 },
    NegativeLeaderEpoch { actual: i32 },
    DuplicateOffset { partition: i32 },
    RetainedBytes,
}

/// Builds one v4 request without coordinator routing or retry policy.
pub(crate) fn txn_offset_commit_v4_request(
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group: TransactionGroupIdentityRef<'_>,
    offsets: &[TransactionOffsetCommitRef<'_>],
) -> Result<TxnOffsetCommitRequest, TxnOffsetCommitRequestFailure> {
    validate_owner(transactional_id, producer_id, producer_epoch)?;
    validate_group(group)?;
    validate_offsets(offsets)?;
    let mut request = TxnOffsetCommitRequest::default();
    request.transactional_id = transactional_id.into();
    request.group_id = group.group_id().into();
    request.producer_id = producer_id;
    request.producer_epoch = producer_epoch;
    request.generation_id_or_member_epoch = group.generation_id_or_member_epoch();
    request.member_id = group.member_id().into();
    request.group_instance_id = group.group_instance_id().map(Into::into);
    request
        .topics
        .try_reserve_exact(unique_topic_count(offsets))
        .map_err(|_| TxnOffsetCommitRequestFailure::RetainedBytes)?;
    for offset in offsets {
        append_offset(&mut request.topics, *offset)?;
    }
    Ok(request)
}

fn validate_owner(
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<(), TxnOffsetCommitRequestFailure> {
    if transactional_id.is_empty() {
        return Err(TxnOffsetCommitRequestFailure::EmptyTransactionalId);
    }
    if producer_id < 0 {
        return Err(TxnOffsetCommitRequestFailure::InvalidProducerId {
            actual: producer_id,
        });
    }
    if producer_epoch < 0 {
        return Err(TxnOffsetCommitRequestFailure::InvalidProducerEpoch {
            actual: producer_epoch,
        });
    }
    Ok(())
}

fn validate_group(
    group: TransactionGroupIdentityRef<'_>,
) -> Result<(), TxnOffsetCommitRequestFailure> {
    if group.group_id().is_empty() {
        return Err(TxnOffsetCommitRequestFailure::EmptyGroupId);
    }
    if group.generation_id_or_member_epoch() < 0 {
        return Err(TxnOffsetCommitRequestFailure::NegativeGroupEpoch {
            actual: group.generation_id_or_member_epoch(),
        });
    }
    if group.member_id().is_empty() {
        return Err(TxnOffsetCommitRequestFailure::EmptyMemberId);
    }
    if group.group_instance_id().is_some_and(str::is_empty) {
        return Err(TxnOffsetCommitRequestFailure::EmptyGroupInstanceId);
    }
    Ok(())
}

fn validate_offsets(
    offsets: &[TransactionOffsetCommitRef<'_>],
) -> Result<(), TxnOffsetCommitRequestFailure> {
    if offsets.is_empty() {
        return Err(TxnOffsetCommitRequestFailure::EmptyOffsets);
    }
    for (index, offset) in offsets.iter().enumerate() {
        if offset.topic().is_empty() {
            return Err(TxnOffsetCommitRequestFailure::EmptyTopic);
        }
        if offset.partition() < 0 {
            return Err(TxnOffsetCommitRequestFailure::NegativePartition {
                actual: offset.partition(),
            });
        }
        if offset.next_offset() < 0 {
            return Err(TxnOffsetCommitRequestFailure::NegativeNextOffset {
                actual: offset.next_offset(),
            });
        }
        if let Some(epoch) = offset.leader_epoch()
            && epoch < 0
        {
            return Err(TxnOffsetCommitRequestFailure::NegativeLeaderEpoch { actual: epoch });
        }
        if offsets[..index].iter().any(|previous| {
            previous.topic() == offset.topic() && previous.partition() == offset.partition()
        }) {
            return Err(TxnOffsetCommitRequestFailure::DuplicateOffset {
                partition: offset.partition(),
            });
        }
    }
    Ok(())
}

pub(super) fn unique_topic_count(offsets: &[TransactionOffsetCommitRef<'_>]) -> usize {
    offsets
        .iter()
        .enumerate()
        .filter(|(index, offset)| {
            !offsets[..*index]
                .iter()
                .any(|previous| previous.topic() == offset.topic())
        })
        .count()
}

fn append_offset(
    topics: &mut Vec<TxnOffsetCommitRequestTopic>,
    offset: TransactionOffsetCommitRef<'_>,
) -> Result<(), TxnOffsetCommitRequestFailure> {
    let partition = request_partition(offset);
    if let Some(topic) = topics
        .iter_mut()
        .find(|topic| topic.name.as_str() == offset.topic())
    {
        topic
            .partitions
            .try_reserve_exact(1)
            .map_err(|_| TxnOffsetCommitRequestFailure::RetainedBytes)?;
        topic.partitions.push(partition);
    } else {
        let mut topic = TxnOffsetCommitRequestTopic::default();
        topic.name = offset.topic().into();
        topic
            .partitions
            .try_reserve_exact(1)
            .map_err(|_| TxnOffsetCommitRequestFailure::RetainedBytes)?;
        topic.partitions.push(partition);
        topics.push(topic);
    }
    Ok(())
}

fn request_partition(offset: TransactionOffsetCommitRef<'_>) -> TxnOffsetCommitRequestPartition {
    let mut partition = TxnOffsetCommitRequestPartition::default();
    partition.partition_index = offset.partition();
    partition.committed_offset = offset.next_offset();
    partition.committed_leader_epoch = offset.leader_epoch().unwrap_or(-1);
    partition.committed_metadata = offset.metadata().map(Into::into);
    partition
}
