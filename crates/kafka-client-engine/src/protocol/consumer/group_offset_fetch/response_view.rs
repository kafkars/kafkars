//! Private common view over generated legacy and modern response DTOs.

use core::num::NonZeroI16;

use kafka_wire::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponsePartitions, OffsetFetchResponseTopic,
    OffsetFetchResponseTopics,
};
use kafka_wire_core::{StrBytes, Uuid};

use super::model::GroupOffsetFetchPartitionValueRef;

pub(super) trait TopicView {
    type Partition: PartitionView;

    fn name(&self) -> &str;
    fn partitions(&self) -> &[Self::Partition];
    fn name_identity_is_representable(&self) -> bool;
}

pub(super) trait PartitionView {
    fn partition_index(&self) -> i32;
    fn committed_offset(&self) -> i64;
    fn committed_leader_epoch(&self) -> i32;
    fn metadata(&self) -> Option<&str>;
    fn error_code(&self) -> i16;
}

pub(super) fn find_topic<'a, T: TopicView>(name: &str, topics: &'a [T]) -> Option<&'a T> {
    topics.iter().find(|topic| topic.name() == name)
}

pub(super) fn find_partition<P: PartitionView>(index: i32, partitions: &[P]) -> Option<&P> {
    partitions
        .iter()
        .find(|partition| partition.partition_index() == index)
}

pub(super) fn partition_value<P: PartitionView>(
    partition: &P,
    selected_version: i16,
) -> GroupOffsetFetchPartitionValueRef<'_> {
    match NonZeroI16::new(partition.error_code()) {
        Some(code) => GroupOffsetFetchPartitionValueRef::Rejected { code },
        None => GroupOffsetFetchPartitionValueRef::Fetched {
            committed_offset: (partition.committed_offset() != -1)
                .then_some(partition.committed_offset()),
            committed_leader_epoch: (selected_version >= 5
                && partition.committed_leader_epoch() != -1)
                .then_some(partition.committed_leader_epoch()),
            metadata: partition.metadata(),
        },
    }
}

macro_rules! impl_partition_view {
    ($type:ty) => {
        impl PartitionView for $type {
            fn partition_index(&self) -> i32 {
                self.partition_index
            }

            fn committed_offset(&self) -> i64 {
                self.committed_offset
            }

            fn committed_leader_epoch(&self) -> i32 {
                self.committed_leader_epoch
            }

            fn metadata(&self) -> Option<&str> {
                self.metadata.as_ref().map(StrBytes::as_str)
            }

            fn error_code(&self) -> i16 {
                self.error_code
            }
        }
    };
}

impl_partition_view!(OffsetFetchResponsePartition);
impl_partition_view!(OffsetFetchResponsePartitions);

impl TopicView for OffsetFetchResponseTopic {
    type Partition = OffsetFetchResponsePartition;

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn partitions(&self) -> &[Self::Partition] {
        &self.partitions
    }

    fn name_identity_is_representable(&self) -> bool {
        true
    }
}

impl TopicView for OffsetFetchResponseTopics {
    type Partition = OffsetFetchResponsePartitions;

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn partitions(&self) -> &[Self::Partition] {
        &self.partitions
    }

    fn name_identity_is_representable(&self) -> bool {
        self.topic_id == Uuid::ZERO
    }
}
