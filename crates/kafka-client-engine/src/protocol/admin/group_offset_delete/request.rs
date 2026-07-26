//! Charged deterministic `OffsetDelete` v0 construction from caller-order targets.

use std::{error::Error, fmt};

use kafka_wire::{
    OffsetDeleteRequest,
    offset_delete_request::{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic},
};

use super::{OffsetDeleteTargetRef, retention::request_grouping_charge};

/// Request construction failure before generated or driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetDeleteRequestFailure {
    RetainedBytes,
}

impl fmt::Display for GroupOffsetDeleteRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetainedBytes => formatter.write_str("request grouping scratch exceeds budget"),
        }
    }
}

impl Error for GroupOffsetDeleteRequestFailure {}

/// Builds one API-key 47 v0 request without acquiring routing or retry policy.
pub(crate) fn group_offset_delete_request(
    group_id: &str,
    targets: &[OffsetDeleteTargetRef<'_>],
    scratch_limit: usize,
) -> Result<OffsetDeleteRequest, GroupOffsetDeleteRequestFailure> {
    let scratch_charge = request_grouping_charge(targets.len())
        .ok_or(GroupOffsetDeleteRequestFailure::RetainedBytes)?;
    if scratch_charge > scratch_limit {
        return Err(GroupOffsetDeleteRequestFailure::RetainedBytes);
    }
    let mut order = Vec::new();
    order
        .try_reserve_exact(targets.len())
        .map_err(|_| GroupOffsetDeleteRequestFailure::RetainedBytes)?;
    order.extend(0..targets.len());
    order.sort_unstable_by(|left, right| {
        targets[*left]
            .topic()
            .as_bytes()
            .cmp(targets[*right].topic().as_bytes())
            .then_with(|| left.cmp(right))
    });

    let mut request = OffsetDeleteRequest::default();
    request.group_id = group_id.into();
    let mut cursor = 0usize;
    while cursor < order.len() {
        let topic_name = targets[order[cursor]].topic();
        let mut topic = OffsetDeleteRequestTopic::default();
        topic.name = topic_name.into();
        while cursor < order.len() && targets[order[cursor]].topic() == topic_name {
            let mut partition = OffsetDeleteRequestPartition::default();
            partition.partition_index = targets[order[cursor]].partition();
            topic.partitions.push(partition);
            cursor += 1;
        }
        request.topics.push(topic);
    }
    Ok(request)
}
