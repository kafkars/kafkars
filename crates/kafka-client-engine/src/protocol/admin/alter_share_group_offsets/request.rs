//! First-topic-appearance request grouping for one destructive API-91 call.

use std::{error::Error, fmt};

use kafka_client_core::AlterShareGroupOffsetsPlan;
use kafka_wire::{
    AlterShareGroupOffsetsRequest,
    alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
    },
};

/// Allocation failure before generated request ownership reaches the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AlterShareGroupOffsetsRequestFailure {
    pub(crate) field: &'static str,
    pub(crate) requested: usize,
}

impl fmt::Display for AlterShareGroupOffsetsRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to reserve {} entries for API-91 {}",
            self.requested, self.field
        )
    }
}

impl Error for AlterShareGroupOffsetsRequestFailure {}

/// Builds one exact-v0 request while preserving first topic and partition order.
pub(crate) fn alter_share_group_offsets_request(
    plan: &AlterShareGroupOffsetsPlan,
) -> Result<AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestFailure> {
    let mut topics: Vec<AlterShareGroupOffsetsRequestTopic> = Vec::new();
    topics
        .try_reserve_exact(plan.changes().len())
        .map_err(|_| AlterShareGroupOffsetsRequestFailure {
            field: "topic groups",
            requested: plan.changes().len(),
        })?;
    for change in plan.changes() {
        let topic_index = topics
            .iter()
            .position(|topic| topic.topic_name.as_str() == change.topic());
        let index = match topic_index {
            Some(index) => index,
            None => {
                let mut topic = AlterShareGroupOffsetsRequestTopic::default();
                topic.topic_name = change.topic().into();
                topics.push(topic);
                topics.len() - 1
            }
        };
        topics[index].partitions.try_reserve(1).map_err(|_| {
            AlterShareGroupOffsetsRequestFailure {
                field: "topic partitions",
                requested: plan.changes().len(),
            }
        })?;
        let mut partition = AlterShareGroupOffsetsRequestPartition::default();
        partition.partition_index = change.partition();
        partition.start_offset = change.start_offset();
        topics[index].partitions.push(partition);
    }

    let mut request = AlterShareGroupOffsetsRequest::default();
    request.group_id = plan.group_id().into();
    request.topics = topics;
    Ok(request)
}
