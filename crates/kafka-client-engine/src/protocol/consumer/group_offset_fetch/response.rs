//! Strict exact-assignment normalization of generated v2-v9 `OffsetFetch`.

use core::num::NonZeroI16;

use kafka_wire::OffsetFetchResponse;

use super::{
    model::{
        GroupOffsetFetchCorrelation, GroupOffsetFetchPartitionValueRef, NormalizedGroupOffsetFetch,
    },
    response_validation::{ensure_limit, matching_group, throttle_time, validate_topics},
    response_view::{PartitionView, TopicView, find_partition, find_topic, partition_value},
    retention::normalized_result_charge,
};

/// Generated response facts unsafe to bind to one assignment bootstrap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetFetchProtocolFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    UnrepresentableThrottleTime { actual: i32 },
    UnexpectedLegacyResults,
    UnexpectedModernResults,
    MissingGroup,
    DuplicateGroup,
    UnexpectedGroup,
    PartitionResultsForGroupError,
    EmptyTopic,
    EmptyTopicPartitions,
    DuplicateTopic,
    UnexpectedTopic,
    MissingTopic,
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    UnexpectedPartition { actual: i32 },
    MissingPartition { actual: i32 },
    InvalidCommittedOffset { actual: i64 },
    InvalidLeaderEpoch { actual: i32 },
    UnrepresentableLeaderEpoch { actual: i32 },
    UnrepresentableTopicId,
    ResultCount,
    RetainedBytes,
}

/// Validates every response fact before allocating caller-ordered result entries.
pub(crate) fn normalize_group_offset_fetch_response<'a>(
    correlation: &GroupOffsetFetchCorrelation,
    response: &'a OffsetFetchResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<NormalizedGroupOffsetFetch<'a>, GroupOffsetFetchProtocolFailure> {
    if !(2..=9).contains(&selected_version) {
        return Err(GroupOffsetFetchProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = throttle_time(response, selected_version)?;
    if selected_version <= 7 {
        if !response.groups.is_empty() {
            return Err(GroupOffsetFetchProtocolFailure::UnexpectedModernResults);
        }
        normalize_topics(
            correlation,
            &response.topics,
            response.error_code,
            throttle_time_ms,
            selected_version,
            result_limit,
        )
    } else {
        if !response.topics.is_empty() || response.error_code != 0 {
            return Err(GroupOffsetFetchProtocolFailure::UnexpectedLegacyResults);
        }
        let group = matching_group(correlation.group_id(), &response.groups)?;
        normalize_topics(
            correlation,
            &group.topics,
            group.error_code,
            throttle_time_ms,
            selected_version,
            result_limit,
        )
    }
}

fn normalize_topics<'a, T: TopicView>(
    correlation: &GroupOffsetFetchCorrelation,
    topics: &'a [T],
    group_error: i16,
    throttle_time_ms: u32,
    selected_version: i16,
    result_limit: usize,
) -> Result<NormalizedGroupOffsetFetch<'a>, GroupOffsetFetchProtocolFailure> {
    if let Some(code) = NonZeroI16::new(group_error) {
        if !topics.is_empty() {
            return Err(GroupOffsetFetchProtocolFailure::PartitionResultsForGroupError);
        }
        let (_, charge) = normalized_result_charge(core::iter::empty())
            .ok_or(GroupOffsetFetchProtocolFailure::RetainedBytes)?;
        ensure_limit(charge, result_limit)?;
        return Ok(NormalizedGroupOffsetFetch::new(
            throttle_time_ms,
            Some(code),
            Vec::new(),
            charge,
        ));
    }
    validate_topics(correlation, topics, selected_version)?;
    let (_, charge) = normalized_result_charge(topics.iter().flat_map(|topic| {
        topic
            .partitions()
            .iter()
            .map(|partition| (partition.error_code(), partition.metadata()))
    }))
    .ok_or(GroupOffsetFetchProtocolFailure::RetainedBytes)?;
    ensure_limit(charge, result_limit)?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(correlation.partition_count())
        .map_err(|_| GroupOffsetFetchProtocolFailure::RetainedBytes)?;
    bind_entries(correlation, topics, selected_version, &mut entries)?;
    Ok(NormalizedGroupOffsetFetch::new(
        throttle_time_ms,
        None,
        entries,
        charge,
    ))
}

fn bind_entries<'a, T: TopicView>(
    correlation: &GroupOffsetFetchCorrelation,
    topics: &'a [T],
    selected_version: i16,
    entries: &mut Vec<GroupOffsetFetchPartitionValueRef<'a>>,
) -> Result<(), GroupOffsetFetchProtocolFailure> {
    for expected_topic in correlation.topics() {
        let topic = find_topic(expected_topic.name(), topics)
            .ok_or(GroupOffsetFetchProtocolFailure::MissingTopic)?;
        for partition_index in expected_topic.partition_indexes() {
            let partition = find_partition(*partition_index, topic.partitions()).ok_or(
                GroupOffsetFetchProtocolFailure::MissingPartition {
                    actual: *partition_index,
                },
            )?;
            entries.push(partition_value(partition, selected_version));
        }
    }
    Ok(())
}
