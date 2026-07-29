//! Bounded API-90 partition validation, correlation, and deterministic ordering.

use core::mem::size_of;
use std::collections::{BTreeMap, BTreeSet};

use super::{
    LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TOPICS,
    LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES,
    ListShareGroupOffsetOutcome, ListShareGroupOffsetResult, ListShareGroupOffsetsBatch,
    ListShareGroupOffsetsBrokerError, ListShareGroupOffsetsPlan, ListShareGroupOffsetsSelection,
};

pub(super) enum ResponseValidation {
    Valid {
        batch: ListShareGroupOffsetsBatch,
        text_bytes: usize,
        retained_bytes: usize,
    },
    TooLarge,
    Invalid,
}

pub(super) fn correlate(
    plan: &ListShareGroupOffsetsPlan,
    batch: ListShareGroupOffsetsBatch,
) -> ResponseValidation {
    let (throttle_time_ms, mut outcomes) = batch.into_parts();
    let (text_bytes, retained_bytes) = match validate_outcomes(&outcomes) {
        Validation::Valid {
            text_bytes,
            retained_bytes,
        } => (text_bytes, retained_bytes),
        Validation::TooLarge => return ResponseValidation::TooLarge,
        Validation::Invalid => return ResponseValidation::Invalid,
    };
    match plan.selection() {
        ListShareGroupOffsetsSelection::All => {
            outcomes.sort_by(|left, right| {
                left.topic()
                    .as_bytes()
                    .cmp(right.topic().as_bytes())
                    .then_with(|| left.partition().cmp(&right.partition()))
            });
        }
        ListShareGroupOffsetsSelection::Selected(targets) => {
            if outcomes.len() != targets.len() {
                return ResponseValidation::Invalid;
            }
            let mut selected = BTreeMap::new();
            for (index, target) in targets.iter().enumerate() {
                selected.insert((target.topic(), target.partition()), index);
            }
            let mut ordered: Vec<Option<ListShareGroupOffsetOutcome>> =
                (0..targets.len()).map(|_| None).collect();
            for outcome in outcomes {
                let Some(index) = selected
                    .get(&(outcome.topic(), outcome.partition()))
                    .copied()
                else {
                    return ResponseValidation::Invalid;
                };
                if ordered[index].replace(outcome).is_some() {
                    return ResponseValidation::Invalid;
                }
            }
            let Some(collected) = ordered.into_iter().collect::<Option<Vec<_>>>() else {
                return ResponseValidation::Invalid;
            };
            outcomes = collected;
        }
    }
    ResponseValidation::Valid {
        batch: ListShareGroupOffsetsBatch::new(throttle_time_ms, outcomes),
        text_bytes,
        retained_bytes,
    }
}

pub(super) fn broker_error_is_valid(error: &ListShareGroupOffsetsBrokerError) -> bool {
    diagnostic_is_valid(error.message(), error.message_truncated())
}

fn validate_outcomes(outcomes: &[ListShareGroupOffsetOutcome]) -> Validation {
    if outcomes.len() > LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS {
        return Validation::TooLarge;
    }
    let mut identities = BTreeSet::new();
    let mut topic_ids = BTreeMap::new();
    let mut text_bytes = 0usize;
    for outcome in outcomes {
        if outcome.topic().is_empty()
            || outcome.topic().len() > LIST_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES
            || outcome.partition() < 0
            || outcome.topic_id() == [0; 16]
        {
            return Validation::Invalid;
        }
        if !identities.insert((outcome.topic(), outcome.partition())) {
            return Validation::Invalid;
        }
        match topic_ids.insert(outcome.topic(), outcome.topic_id()) {
            Some(topic_id) if topic_id != outcome.topic_id() => return Validation::Invalid,
            _ => {}
        }
        if topic_ids.len() > LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TOPICS {
            return Validation::TooLarge;
        }
        let diagnostic_bytes = match outcome.result() {
            ListShareGroupOffsetResult::Described(description) => {
                if description.start_offset().is_some_and(|value| value < 0)
                    || description.leader_epoch().is_some_and(|value| value < 0)
                    || description.lag().is_some_and(|value| value < 0)
                {
                    return Validation::Invalid;
                }
                0
            }
            ListShareGroupOffsetResult::Failed(error) => {
                if !diagnostic_is_valid(error.message(), error.message_truncated()) {
                    return Validation::Invalid;
                }
                error.message().map_or(0, str::len)
            }
        };
        let Some(total) = text_bytes
            .checked_add(outcome.topic().len())
            .and_then(|total| total.checked_add(diagnostic_bytes))
        else {
            return Validation::TooLarge;
        };
        text_bytes = total;
        if text_bytes > LIST_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES {
            return Validation::TooLarge;
        }
    }
    let Some(outcome_bytes) = outcomes
        .len()
        .checked_mul(size_of::<ListShareGroupOffsetOutcome>())
    else {
        return Validation::TooLarge;
    };
    let Some(retained_bytes) = size_of::<ListShareGroupOffsetsBatch>()
        .checked_add(outcome_bytes)
        .and_then(|total| total.checked_add(text_bytes))
    else {
        return Validation::TooLarge;
    };
    if retained_bytes > LIST_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES {
        return Validation::TooLarge;
    }
    Validation::Valid {
        text_bytes,
        retained_bytes,
    }
}

fn diagnostic_is_valid(message: Option<&str>, message_truncated: bool) -> bool {
    message.is_none_or(|message| message.len() <= LIST_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES)
        && (message.is_some() || !message_truncated)
}

enum Validation {
    Valid {
        text_bytes: usize,
        retained_bytes: usize,
    },
    TooLarge,
    Invalid,
}
