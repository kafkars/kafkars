//! Strict bounded correlation for one API-91 share-group response.

use std::collections::BTreeMap;

use super::{
    ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES,
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError,
    AlterShareGroupOffsetsPartitionOutcome, AlterShareGroupOffsetsPartitionResult,
    AlterShareGroupOffsetsPlan,
};

pub(super) enum ResponseValidation {
    Valid(AlterShareGroupOffsetsBatch),
    TooLarge,
    Invalid,
}

pub(super) fn correlate_response(
    plan: &AlterShareGroupOffsetsPlan,
    batch: AlterShareGroupOffsetsBatch,
) -> ResponseValidation {
    let (throttle_time_ms, outcomes) = batch.into_parts();
    if outcomes.len() > ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS
        || outcomes.len() > ALTER_SHARE_GROUP_OFFSETS_MAX_PARTITIONS
    {
        return ResponseValidation::TooLarge;
    }
    if outcomes.len() != plan.changes().len() {
        return ResponseValidation::Invalid;
    }

    let mut text_bytes = 0usize;
    let mut by_identity = BTreeMap::new();
    for outcome in outcomes {
        let (topic, topic_id, partition, result) = outcome.into_parts();
        if topic.is_empty()
            || topic.len() > ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES
            || partition < 0
            || topic_id == [0; 16]
        {
            return ResponseValidation::Invalid;
        }
        let diagnostic = match &result {
            AlterShareGroupOffsetsPartitionResult::Altered => None,
            AlterShareGroupOffsetsPartitionResult::Failed(error) => {
                if !diagnostic_is_valid(error.message(), error.message_truncated()) {
                    return ResponseValidation::Invalid;
                }
                error.message()
            }
        };
        let Some(total) = text_bytes
            .checked_add(topic.len())
            .and_then(|total| total.checked_add(diagnostic.map_or(0, str::len)))
        else {
            return ResponseValidation::TooLarge;
        };
        text_bytes = total;
        if text_bytes > ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES {
            return ResponseValidation::TooLarge;
        }
        if by_identity
            .insert((topic, partition), (topic_id, result))
            .is_some()
        {
            return ResponseValidation::Invalid;
        }
    }

    let Some(outcome_bytes) = plan
        .changes()
        .len()
        .checked_mul(core::mem::size_of::<AlterShareGroupOffsetsPartitionOutcome>())
    else {
        return ResponseValidation::TooLarge;
    };
    let Some(retained_bytes) = text_bytes.checked_add(outcome_bytes) else {
        return ResponseValidation::TooLarge;
    };
    if retained_bytes > ALTER_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES {
        return ResponseValidation::TooLarge;
    }

    let mut ordered = Vec::with_capacity(plan.changes().len());
    for change in plan.changes() {
        let key = (change.topic().to_owned(), change.partition());
        let Some((topic_id, result)) = by_identity.remove(&key) else {
            return ResponseValidation::Invalid;
        };
        ordered.push(match result {
            AlterShareGroupOffsetsPartitionResult::Altered => {
                AlterShareGroupOffsetsPartitionOutcome::altered(key.0, topic_id, key.1)
            }
            AlterShareGroupOffsetsPartitionResult::Failed(error) => {
                AlterShareGroupOffsetsPartitionOutcome::failed(key.0, topic_id, key.1, error)
            }
        });
    }
    if !by_identity.is_empty() {
        return ResponseValidation::Invalid;
    }
    ResponseValidation::Valid(AlterShareGroupOffsetsBatch::new(throttle_time_ms, ordered))
}

pub(super) fn broker_error_is_valid(error: &AlterShareGroupOffsetsBrokerError) -> bool {
    diagnostic_is_valid(error.message(), error.message_truncated())
}

fn diagnostic_is_valid(message: Option<&str>, message_truncated: bool) -> bool {
    message.is_none_or(|message| message.len() <= ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES)
        && (message.is_some() || !message_truncated)
}
