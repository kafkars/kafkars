//! Charged generated `OffsetCommit` construction from caller-order alterations.

use std::{error::Error, fmt};

use kafka_wire::{
    OffsetCommitRequest,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};

use super::{OffsetCommitTargetRef, retention::generated_request_peak_charge};

/// Request construction failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetAlterRequestFailure {
    RetainedBytes,
}

impl fmt::Display for GroupOffsetAlterRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetainedBytes => {
                formatter.write_str("generated request peak exceeds its proven budget")
            }
        }
    }
}

impl Error for GroupOffsetAlterRequestFailure {}

/// Builds one non-member API-key 8 request without routing or retry policy.
pub(crate) fn group_offset_alter_request(
    group_id: &str,
    targets: &[OffsetCommitTargetRef<'_>],
    retention_time_ms: Option<i64>,
    scratch_limit: usize,
) -> Result<OffsetCommitRequest, GroupOffsetAlterRequestFailure> {
    let scratch_charge = generated_request_peak_charge(group_id, targets.iter().copied())
        .ok_or(GroupOffsetAlterRequestFailure::RetainedBytes)?;
    if scratch_charge > scratch_limit {
        return Err(GroupOffsetAlterRequestFailure::RetainedBytes);
    }
    let order = grouped_order(targets)?;
    let mut request = OffsetCommitRequest::default();
    request.group_id = group_id.into();
    request.generation_id_or_member_epoch = -1;
    request.member_id = "".into();
    request.group_instance_id = None;
    request.retention_time_ms = retention_time_ms.unwrap_or(-1);
    append_topics(&mut request, targets, &order);
    Ok(request)
}

fn grouped_order(
    targets: &[OffsetCommitTargetRef<'_>],
) -> Result<Vec<usize>, GroupOffsetAlterRequestFailure> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(targets.len())
        .map_err(|_| GroupOffsetAlterRequestFailure::RetainedBytes)?;
    order.extend(0..targets.len());
    order.sort_unstable_by(|left, right| {
        targets[*left]
            .topic()
            .as_bytes()
            .cmp(targets[*right].topic().as_bytes())
            .then_with(|| left.cmp(right))
    });
    Ok(order)
}

fn append_topics(
    request: &mut OffsetCommitRequest,
    targets: &[OffsetCommitTargetRef<'_>],
    order: &[usize],
) {
    let mut cursor = 0usize;
    while cursor < order.len() {
        let topic_name = targets[order[cursor]].topic();
        let mut topic = OffsetCommitRequestTopic::default();
        topic.name = topic_name.into();
        while cursor < order.len() && targets[order[cursor]].topic() == topic_name {
            topic
                .partitions
                .push(request_partition(targets[order[cursor]]));
            cursor += 1;
        }
        request.topics.push(topic);
    }
}

fn request_partition(target: OffsetCommitTargetRef<'_>) -> OffsetCommitRequestPartition {
    let mut partition = OffsetCommitRequestPartition::default();
    partition.partition_index = target.partition();
    partition.committed_offset = target.next_offset();
    partition.committed_leader_epoch = target.leader_epoch().unwrap_or(-1);
    partition.committed_metadata = target.metadata().map(Into::into);
    partition
}
