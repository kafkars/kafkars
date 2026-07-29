//! Explicit API-92 source-text, diagnostic, scratch, and terminal bounds.

use core::mem::size_of;

use kafka_client_core::{
    DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES,
    DELETE_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, DeleteShareGroupOffsetsBatch,
    DeleteShareGroupOffsetsBrokerError, DeleteShareGroupOffsetsTopicBrokerError,
    DeleteShareGroupOffsetsTopicOutcome,
};

use super::ValidatedDeleteShareGroupOffsetsResponse;

pub(super) const MAX_DIAGNOSTIC_BYTES: usize = DELETE_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize =
    DELETE_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES;
pub(super) const MAX_NORMALIZED_BYTES: usize = DELETE_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES;
pub(super) const MAX_RESPONSE_TOPICS: usize = 4 * 1024;
pub(super) const MAX_TOPIC_NAME_BYTES: usize = i16::MAX as usize;

pub(super) fn bounded_diagnostic(source: Option<&str>) -> (Option<String>, bool) {
    let Some(source) = source else {
        return (None, false);
    };
    let mut end = source.len().min(MAX_DIAGNOSTIC_BYTES);
    while !source.is_char_boundary(end) {
        end -= 1;
    }
    (Some(source[..end].to_owned()), end < source.len())
}

pub(super) fn batch_required_bytes<'a>(
    topics: impl Iterator<Item = (&'a str, Option<&'a str>)>,
) -> Option<usize> {
    let mut required = size_of::<ValidatedDeleteShareGroupOffsetsResponse>()
        .checked_add(size_of::<DeleteShareGroupOffsetsBatch>())?;
    for (topic, diagnostic) in topics {
        required = required
            .checked_add(size_of::<DeleteShareGroupOffsetsTopicOutcome>())?
            .checked_add(topic.len())?
            .checked_add(diagnostic.map_or(0, |message| message.len().min(MAX_DIAGNOSTIC_BYTES)))?;
    }
    Some(required)
}

pub(super) fn broker_error_required_bytes(message: Option<&str>) -> Option<usize> {
    size_of::<ValidatedDeleteShareGroupOffsetsResponse>()
        .checked_add(size_of::<DeleteShareGroupOffsetsBrokerError>())?
        .checked_add(message.map_or(0, |value| value.len().min(MAX_DIAGNOSTIC_BYTES)))
}

pub(super) fn correlation_scratch_bytes(count: usize) -> Option<usize> {
    let vector_headers = 3usize.checked_mul(size_of::<Vec<usize>>())?;
    vector_headers.checked_add(3usize.checked_mul(count.checked_mul(size_of::<usize>())?)?)
}

pub(super) fn actual_batch_retained_bytes(batch: &DeleteShareGroupOffsetsBatch) -> Option<usize> {
    let mut retained = size_of::<ValidatedDeleteShareGroupOffsetsResponse>()
        .checked_add(size_of::<DeleteShareGroupOffsetsBatch>())?
        .checked_add(
            batch
                .outcomes()
                .len()
                .checked_mul(size_of::<DeleteShareGroupOffsetsTopicOutcome>())?,
        )?;
    for outcome in batch.outcomes() {
        retained = retained.checked_add(outcome.topic().len())?;
        if let kafka_client_core::DeleteShareGroupOffsetsTopicResult::Failed(error) =
            outcome.result()
        {
            retained = retained.checked_add(error.message().map_or(0, str::len))?;
        }
    }
    Some(retained)
}

pub(super) fn actual_broker_error_retained_bytes(
    error: &DeleteShareGroupOffsetsBrokerError,
) -> Option<usize> {
    size_of::<ValidatedDeleteShareGroupOffsetsResponse>()
        .checked_add(size_of::<DeleteShareGroupOffsetsBrokerError>())?
        .checked_add(error.message().map_or(0, str::len))
}

pub(super) fn topic_error_required_bytes(message: Option<&str>) -> Option<usize> {
    size_of::<DeleteShareGroupOffsetsTopicBrokerError>()
        .checked_add(message.map_or(0, |value| value.len().min(MAX_DIAGNOSTIC_BYTES)))
}
