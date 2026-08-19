//! Nullable-all or first-topic-appearance selected API-90 request construction.

use std::{error::Error, fmt};

use kafka_client_core::{ListShareGroupOffsetsPlan, ListShareGroupOffsetsSelection};
use kafka_wire::{
    DescribeShareGroupOffsetsRequest,
    describe_share_group_offsets_request::{
        DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
    },
};

/// Allocation failure before generated request ownership reaches the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ListShareGroupOffsetsRequestFailure {
    pub(crate) field: &'static str,
    pub(crate) requested: usize,
}

impl fmt::Display for ListShareGroupOffsetsRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to reserve {} entries for API-90 {}",
            self.requested, self.field
        )
    }
}

impl Error for ListShareGroupOffsetsRequestFailure {}

/// Builds exactly one generated request group without routing or retry policy.
pub(crate) fn list_share_group_offsets_request(
    plan: &ListShareGroupOffsetsPlan,
) -> Result<DescribeShareGroupOffsetsRequest, ListShareGroupOffsetsRequestFailure> {
    let topics = match plan.selection() {
        ListShareGroupOffsetsSelection::All => None,
        ListShareGroupOffsetsSelection::Selected(targets) => Some(group_selected_topics(targets)?),
    };
    let mut group = DescribeShareGroupOffsetsRequestGroup::default();
    group.group_id = plan.group_id().into();
    group.topics = topics;
    let mut request = DescribeShareGroupOffsetsRequest::default();
    request.groups = vec![group];
    Ok(request)
}

fn group_selected_topics(
    targets: &[kafka_client_core::ListShareGroupOffsetTarget],
) -> Result<Vec<DescribeShareGroupOffsetsRequestTopic>, ListShareGroupOffsetsRequestFailure> {
    let mut topics: Vec<DescribeShareGroupOffsetsRequestTopic> = Vec::new();
    topics
        .try_reserve_exact(targets.len())
        .map_err(|_| ListShareGroupOffsetsRequestFailure {
            field: "topic groups",
            requested: targets.len(),
        })?;
    for target in targets {
        let topic_index = topics
            .iter()
            .position(|topic| topic.topic_name.as_str() == target.topic());
        let index = if let Some(index) = topic_index {
            index
        } else {
            let mut topic = DescribeShareGroupOffsetsRequestTopic::default();
            topic.topic_name = target.topic().into();
            topics.push(topic);
            topics.len() - 1
        };
        topics[index].partitions.try_reserve(1).map_err(|_| {
            ListShareGroupOffsetsRequestFailure {
                field: "selected partitions",
                requested: targets.len(),
            }
        })?;
        topics[index].partitions.push(target.partition());
    }
    Ok(topics)
}
