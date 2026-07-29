//! Bounded linear validation plus one charged borrowed sort for `OffsetFetch`.

use core::num::NonZeroI16;

use kafka_client_core::ListConsumerGroupOffsetsSelection;
use kafka_wire::{
    OffsetFetchResponse,
    offset_fetch_response::{
        OffsetFetchResponseGroup, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};

use super::{
    entries::{collect_legacy_entries, collect_modern_entries, reject_sorted_duplicates},
    model::{ValidatedGroupOffsetsResponse, group_offset_order},
    retention::validated_result_charge,
    shape::{validate_legacy_topics, validate_modern_topics},
};

/// Generated response facts unsafe to bind to an admin operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetsProtocolFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    UnexpectedLegacyResults,
    UnexpectedMultiGroupResults,
    MissingGroup,
    UnexpectedGroup,
    DuplicateGroup,
    EmptyTopic,
    EmptyTopicPartitions,
    DuplicateTopic,
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    InvalidCommittedOffset { actual: i64 },
    InvalidLeaderEpoch { actual: i32 },
    SelectedPartitionCountMismatch { expected: usize, actual: usize },
    UnexpectedSelectedPartition,
    RetainedBytes,
}

/// Validates one all-partition response using Kafka's deterministic result order.
pub(crate) fn validate_group_offsets_response<'a>(
    expected_group: &str,
    response: &'a OffsetFetchResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedGroupOffsetsResponse<'a>, GroupOffsetsProtocolFailure> {
    validate_group_offsets_response_for_selection(
        expected_group,
        &ListConsumerGroupOffsetsSelection::All,
        response,
        selected_version,
        result_limit,
    )
}

/// Validates linearly, proves capacity, and restores the selection's result order.
pub(crate) fn validate_group_offsets_response_for_selection<'a>(
    expected_group: &str,
    selection: &ListConsumerGroupOffsetsSelection,
    response: &'a OffsetFetchResponse,
    selected_version: i16,
    result_limit: usize,
) -> Result<ValidatedGroupOffsetsResponse<'a>, GroupOffsetsProtocolFailure> {
    if !(2..=9).contains(&selected_version) {
        return Err(GroupOffsetsProtocolFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    let throttle_time_ms = throttle_time(response, selected_version)?;
    let (mut entries, top_level_error, retained_charge) = if selected_version <= 7 {
        if !response.groups.is_empty() {
            return Err(GroupOffsetsProtocolFailure::UnexpectedMultiGroupResults);
        }
        if let Some(code) = NonZeroI16::new(response.error_code) {
            return broker_rejection(response, throttle_time_ms, code, result_limit);
        }
        validate_legacy_topics(&response.topics, selected_version)?;
        let (count, charge) = legacy_charge(&response.topics)?;
        ensure_limit(charge, result_limit)?;
        (
            collect_legacy_entries(&response.topics, selected_version, count)?,
            NonZeroI16::new(response.error_code),
            charge,
        )
    } else {
        if !response.topics.is_empty() || response.error_code != 0 {
            return Err(GroupOffsetsProtocolFailure::UnexpectedLegacyResults);
        }
        let group = matching_group(expected_group, &response.groups)?;
        if let Some(code) = NonZeroI16::new(group.error_code) {
            return broker_rejection(response, throttle_time_ms, code, result_limit);
        }
        validate_modern_topics(&group.topics, selected_version)?;
        let (count, charge) = modern_charge(&group.topics)?;
        ensure_limit(charge, result_limit)?;
        (
            collect_modern_entries(&group.topics, selected_version, count)?,
            NonZeroI16::new(group.error_code),
            charge,
        )
    };
    entries.sort_unstable_by(group_offset_order);
    reject_sorted_duplicates(&entries)?;
    restore_selection_order(&mut entries, selection)?;
    Ok(ValidatedGroupOffsetsResponse::new(
        entries,
        throttle_time_ms,
        top_level_error,
        retained_charge,
    ))
}

fn restore_selection_order(
    entries: &mut [super::model::BorrowedGroupOffset<'_>],
    selection: &ListConsumerGroupOffsetsSelection,
) -> Result<(), GroupOffsetsProtocolFailure> {
    let ListConsumerGroupOffsetsSelection::Selected(targets) = selection else {
        return Ok(());
    };
    if entries.len() != targets.len() {
        return Err(
            GroupOffsetsProtocolFailure::SelectedPartitionCountMismatch {
                expected: targets.len(),
                actual: entries.len(),
            },
        );
    }
    for (caller_position, target) in targets.iter().enumerate() {
        let entry_index = entries
            .binary_search_by(|entry| {
                entry
                    .topic()
                    .as_bytes()
                    .cmp(target.topic().as_bytes())
                    .then_with(|| entry.partition().cmp(&target.partition()))
            })
            .map_err(|_| GroupOffsetsProtocolFailure::UnexpectedSelectedPartition)?;
        entries[entry_index].set_caller_position(caller_position);
    }
    entries.sort_unstable_by_key(|entry| entry.caller_position());
    Ok(())
}

fn broker_rejection(
    _response: &OffsetFetchResponse,
    throttle_time_ms: u32,
    code: NonZeroI16,
    result_limit: usize,
) -> Result<ValidatedGroupOffsetsResponse<'_>, GroupOffsetsProtocolFailure> {
    let (_, retained_charge) = validated_result_charge(core::iter::empty())
        .ok_or(GroupOffsetsProtocolFailure::RetainedBytes)?;
    ensure_limit(retained_charge, result_limit)?;
    Ok(ValidatedGroupOffsetsResponse::new(
        Vec::new(),
        throttle_time_ms,
        Some(code),
        retained_charge,
    ))
}

fn throttle_time(
    response: &OffsetFetchResponse,
    selected_version: i16,
) -> Result<u32, GroupOffsetsProtocolFailure> {
    if selected_version < 3 {
        return Ok(0);
    }
    u32::try_from(response.throttle_time_ms).map_err(|_| {
        GroupOffsetsProtocolFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })
}

fn matching_group<'a>(
    expected: &str,
    groups: &'a [OffsetFetchResponseGroup],
) -> Result<&'a OffsetFetchResponseGroup, GroupOffsetsProtocolFailure> {
    let mut matching = None;
    for group in groups {
        if group.group_id.as_str() != expected {
            return Err(GroupOffsetsProtocolFailure::UnexpectedGroup);
        }
        if matching.replace(group).is_some() {
            return Err(GroupOffsetsProtocolFailure::DuplicateGroup);
        }
    }
    matching.ok_or(GroupOffsetsProtocolFailure::MissingGroup)
}

fn ensure_limit(charge: usize, limit: usize) -> Result<(), GroupOffsetsProtocolFailure> {
    (charge <= limit)
        .then_some(())
        .ok_or(GroupOffsetsProtocolFailure::RetainedBytes)
}

fn legacy_charge(
    topics: &[OffsetFetchResponseTopic],
) -> Result<(usize, usize), GroupOffsetsProtocolFailure> {
    validated_result_charge(topics.iter().flat_map(|topic| {
        topic.partitions.iter().map(|partition| {
            (
                topic.name.as_str(),
                partition.error_code,
                partition
                    .metadata
                    .as_ref()
                    .map(kafka_wire_core::StrBytes::as_str),
            )
        })
    }))
    .ok_or(GroupOffsetsProtocolFailure::RetainedBytes)
}

fn modern_charge(
    topics: &[OffsetFetchResponseTopics],
) -> Result<(usize, usize), GroupOffsetsProtocolFailure> {
    validated_result_charge(topics.iter().flat_map(|topic| {
        topic.partitions.iter().map(|partition| {
            (
                topic.name.as_str(),
                partition.error_code,
                partition
                    .metadata
                    .as_ref()
                    .map(kafka_wire_core::StrBytes::as_str),
            )
        })
    }))
    .ok_or(GroupOffsetsProtocolFailure::RetainedBytes)
}
