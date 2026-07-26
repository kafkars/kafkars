//! Allocation-free envelope, identity, and scalar response validation.

use kafka_wire::{OffsetFetchResponse, offset_fetch_response::OffsetFetchResponseGroup};

use super::{
    model::{GroupOffsetFetchCorrelation, GroupOffsetFetchTopic},
    response::GroupOffsetFetchProtocolFailure,
    response_view::{PartitionView, TopicView, find_partition, find_topic},
};

pub(super) fn validate_topics<T: TopicView>(
    correlation: &GroupOffsetFetchCorrelation,
    topics: &[T],
    selected_version: i16,
) -> Result<(), GroupOffsetFetchProtocolFailure> {
    let mut result_count = 0usize;
    for (topic_index, topic) in topics.iter().enumerate() {
        if topic.name().is_empty() {
            return Err(GroupOffsetFetchProtocolFailure::EmptyTopic);
        }
        if topic.partitions().is_empty() {
            return Err(GroupOffsetFetchProtocolFailure::EmptyTopicPartitions);
        }
        if !topic.name_identity_is_representable() {
            return Err(GroupOffsetFetchProtocolFailure::UnrepresentableTopicId);
        }
        if topics[..topic_index]
            .iter()
            .any(|previous| previous.name() == topic.name())
        {
            return Err(GroupOffsetFetchProtocolFailure::DuplicateTopic);
        }
        let expected = find_expected_topic(topic.name(), correlation.topics())
            .ok_or(GroupOffsetFetchProtocolFailure::UnexpectedTopic)?;
        result_count = result_count
            .checked_add(topic.partitions().len())
            .ok_or(GroupOffsetFetchProtocolFailure::ResultCount)?;
        validate_partitions(expected, topic.partitions(), selected_version)?;
    }
    validate_missing(correlation, topics)?;
    if result_count != correlation.partition_count() {
        return Err(GroupOffsetFetchProtocolFailure::ResultCount);
    }
    Ok(())
}

fn validate_missing<T: TopicView>(
    correlation: &GroupOffsetFetchCorrelation,
    topics: &[T],
) -> Result<(), GroupOffsetFetchProtocolFailure> {
    for expected in correlation.topics() {
        let topic = find_topic(expected.name(), topics)
            .ok_or(GroupOffsetFetchProtocolFailure::MissingTopic)?;
        for partition in expected.partition_indexes() {
            if find_partition(*partition, topic.partitions()).is_none() {
                return Err(GroupOffsetFetchProtocolFailure::MissingPartition {
                    actual: *partition,
                });
            }
        }
    }
    Ok(())
}

fn validate_partitions<P: PartitionView>(
    expected: &GroupOffsetFetchTopic,
    partitions: &[P],
    selected_version: i16,
) -> Result<(), GroupOffsetFetchProtocolFailure> {
    for (index, partition) in partitions.iter().enumerate() {
        let partition_index = partition.partition_index();
        if partition_index < 0 {
            return Err(GroupOffsetFetchProtocolFailure::NegativePartition {
                actual: partition_index,
            });
        }
        if partitions[..index]
            .iter()
            .any(|previous| previous.partition_index() == partition_index)
        {
            return Err(GroupOffsetFetchProtocolFailure::DuplicatePartition {
                actual: partition_index,
            });
        }
        if !expected.partition_indexes().contains(&partition_index) {
            return Err(GroupOffsetFetchProtocolFailure::UnexpectedPartition {
                actual: partition_index,
            });
        }
        validate_partition_value(partition, selected_version)?;
    }
    Ok(())
}

fn validate_partition_value<P: PartitionView>(
    partition: &P,
    selected_version: i16,
) -> Result<(), GroupOffsetFetchProtocolFailure> {
    if selected_version < 5 && partition.committed_leader_epoch() != -1 {
        return Err(
            GroupOffsetFetchProtocolFailure::UnrepresentableLeaderEpoch {
                actual: partition.committed_leader_epoch(),
            },
        );
    }
    if partition.error_code() != 0 {
        return Ok(());
    }
    if partition.committed_offset() < -1 {
        return Err(GroupOffsetFetchProtocolFailure::InvalidCommittedOffset {
            actual: partition.committed_offset(),
        });
    }
    if selected_version >= 5 && partition.committed_leader_epoch() < -1 {
        return Err(GroupOffsetFetchProtocolFailure::InvalidLeaderEpoch {
            actual: partition.committed_leader_epoch(),
        });
    }
    Ok(())
}

pub(super) fn throttle_time(
    response: &OffsetFetchResponse,
    selected_version: i16,
) -> Result<u32, GroupOffsetFetchProtocolFailure> {
    let throttle = u32::try_from(response.throttle_time_ms).map_err(|_| {
        GroupOffsetFetchProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    if selected_version == 2 && throttle != 0 {
        return Err(
            GroupOffsetFetchProtocolFailure::UnrepresentableThrottleTime {
                actual: response.throttle_time_ms,
            },
        );
    }
    Ok(throttle)
}

pub(super) fn matching_group<'a>(
    expected: &str,
    groups: &'a [OffsetFetchResponseGroup],
) -> Result<&'a OffsetFetchResponseGroup, GroupOffsetFetchProtocolFailure> {
    let [group] = groups else {
        if groups.is_empty() {
            return Err(GroupOffsetFetchProtocolFailure::MissingGroup);
        }
        if groups
            .iter()
            .all(|group| group.group_id.as_str() == expected)
        {
            return Err(GroupOffsetFetchProtocolFailure::DuplicateGroup);
        }
        return Err(GroupOffsetFetchProtocolFailure::UnexpectedGroup);
    };
    if group.group_id.as_str() != expected {
        return Err(GroupOffsetFetchProtocolFailure::UnexpectedGroup);
    }
    Ok(group)
}

pub(super) fn ensure_limit(
    charge: usize,
    limit: usize,
) -> Result<(), GroupOffsetFetchProtocolFailure> {
    (charge <= limit)
        .then_some(())
        .ok_or(GroupOffsetFetchProtocolFailure::RetainedBytes)
}

fn find_expected_topic<'a>(
    name: &str,
    topics: &'a [GroupOffsetFetchTopic],
) -> Option<&'a GroupOffsetFetchTopic> {
    topics.iter().find(|topic| topic.name() == name)
}
