//! Explicit API-91 source-text, diagnostic, scratch, and terminal bounds.

use core::{cmp::Ordering, mem::size_of};

use kafka_client_core::{
    ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES,
    ALTER_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES, ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES,
    AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError,
    AlterShareGroupOffsetsPartitionOutcome,
};

use super::ValidatedAlterShareGroupOffsetsResponse;

pub(super) const MAX_DIAGNOSTIC_BYTES: usize = ALTER_SHARE_GROUP_OFFSETS_DIAGNOSTIC_BYTES;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_TEXT_BYTES;
pub(super) const MAX_NORMALIZED_BYTES: usize = ALTER_SHARE_GROUP_OFFSETS_MAX_RETAINED_BYTES;
pub(super) const MAX_RESPONSE_PARTITIONS: usize = ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS;
pub(super) const MAX_RESPONSE_TOPICS: usize = ALTER_SHARE_GROUP_OFFSETS_MAX_RESPONSE_PARTITIONS;
pub(super) const MAX_TOPIC_NAME_BYTES: usize = ALTER_SHARE_GROUP_OFFSETS_MAX_TOPIC_NAME_BYTES;

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
    partitions: impl Iterator<Item = (&'a str, Option<&'a str>)>,
) -> Option<usize> {
    let mut required = size_of::<ValidatedAlterShareGroupOffsetsResponse>()
        .checked_add(size_of::<AlterShareGroupOffsetsBatch>())?;
    for (topic, diagnostic) in partitions {
        required = required
            .checked_add(size_of::<AlterShareGroupOffsetsPartitionOutcome>())?
            .checked_add(topic.len())?
            .checked_add(diagnostic.map_or(0, |message| message.len().min(MAX_DIAGNOSTIC_BYTES)))?;
    }
    Some(required)
}

pub(super) fn broker_error_required_bytes(message: Option<&str>) -> Option<usize> {
    size_of::<ValidatedAlterShareGroupOffsetsResponse>()
        .checked_add(size_of::<AlterShareGroupOffsetsBrokerError>())?
        .checked_add(message.map_or(0, |value| value.len().min(MAX_DIAGNOSTIC_BYTES)))
}

pub(super) fn correlation_scratch_bytes(count: usize) -> Option<usize> {
    let vector_headers = 3usize.checked_mul(size_of::<Vec<usize>>())?;
    vector_headers.checked_add(16usize.checked_mul(count.checked_mul(size_of::<usize>())?)?)
}

pub(super) fn partition_identity_cmp(
    left_topic: &str,
    left_partition: i32,
    right_topic: &str,
    right_partition: i32,
) -> Ordering {
    left_topic
        .as_bytes()
        .cmp(right_topic.as_bytes())
        .then_with(|| left_partition.cmp(&right_partition))
}
