//! Explicit API-90 source-text, diagnostic, scratch, and terminal bounds.

use core::mem::size_of;

use kafka_client_core::{
    ListShareGroupOffsetOutcome, ListShareGroupOffsetsBatch, ListShareGroupOffsetsBrokerError,
};

use super::ValidatedListShareGroupOffsetsResponse;

pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 1024;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = 1024 * 1024;
pub(super) const MAX_NORMALIZED_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_RESPONSE_TOPICS: usize = 4 * 1024;
pub(super) const MAX_RESPONSE_PARTITIONS: usize = 16 * 1024;
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

pub(super) fn broker_error_required_bytes(message: Option<&str>) -> Option<usize> {
    size_of::<ValidatedListShareGroupOffsetsResponse>()
        .checked_add(size_of::<ListShareGroupOffsetsBrokerError>())?
        .checked_add(message.map_or(0, |value| value.len().min(MAX_DIAGNOSTIC_BYTES)))
}

pub(super) fn batch_required_bytes<'a>(
    entries: impl Iterator<Item = (&'a str, Option<&'a str>)>,
) -> Option<usize> {
    let mut required = size_of::<ValidatedListShareGroupOffsetsResponse>()
        .checked_add(size_of::<ListShareGroupOffsetsBatch>())?;
    for (topic, diagnostic) in entries {
        required = required
            .checked_add(size_of::<ListShareGroupOffsetOutcome>())?
            .checked_add(topic.len())?
            .checked_add(diagnostic.map_or(0, |message| message.len().min(MAX_DIAGNOSTIC_BYTES)))?;
    }
    Some(required)
}

pub(super) fn scratch_required_bytes(partitions: usize) -> Option<usize> {
    let entries =
        partitions.checked_mul(size_of::<super::correlation::BorrowedPartition<'static>>())?;
    let expected =
        partitions.checked_mul(size_of::<super::correlation::IndexedTarget<'static>>())?;
    let caller_order = partitions.checked_mul(size_of::<(
        usize,
        super::correlation::BorrowedPartition<'static>,
    )>())?;
    size_of::<Vec<super::correlation::BorrowedPartition<'static>>>()
        .checked_add(size_of::<Vec<super::correlation::IndexedTarget<'static>>>())?
        .checked_add(size_of::<
            Vec<(usize, super::correlation::BorrowedPartition<'static>)>,
        >())?
        .checked_add(entries)?
        .checked_add(expected)?
        .checked_add(caller_order)
}
