//! Allocation-free capacity proof for request grouping and normalized results.

use core::{mem::size_of, num::NonZeroI16};

use kafka_wire::{
    OffsetCommitRequest,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};

use super::{
    OffsetCommitPartitionRef, OffsetCommitTargetRef,
    correlation::{BorrowedOffsetCommitPartition, IndexedOffsetCommitTarget},
};

type OwnedOffsetCommitEntryCharge = (String, i32, Result<(), NonZeroI16>);

const BASE_RESULT_CHARGE: usize = size_of::<Vec<OwnedOffsetCommitEntryCharge>>()
    + size_of::<Vec<OffsetCommitPartitionRef<'static>>>()
    + size_of::<Vec<IndexedOffsetCommitTarget<'static>>>()
    + size_of::<Vec<BorrowedOffsetCommitPartition<'static>>>()
    + size_of::<u32>();
const OWNED_ENTRY_CHARGE: usize = size_of::<OwnedOffsetCommitEntryCharge>();
const CORRELATION_ENTRY_CHARGE: usize = size_of::<OffsetCommitPartitionRef<'static>>();
const EXPECTED_SORT_ENTRY_CHARGE: usize = size_of::<IndexedOffsetCommitTarget<'static>>();
const RESPONSE_SORT_ENTRY_CHARGE: usize = size_of::<BorrowedOffsetCommitPartition<'static>>();
const REQUEST_SORT_ENTRY_CHARGE: usize = size_of::<usize>();
const GENERATED_TOPIC_CHARGE: usize = size_of::<OffsetCommitRequestTopic>();
const GENERATED_PARTITION_CHARGE: usize = size_of::<OffsetCommitRequestPartition>();

pub(super) fn validated_result_charge<'a>(
    targets: impl Iterator<Item = &'a str>,
) -> Option<(usize, usize)> {
    let mut charge = BASE_RESULT_CHARGE;
    let mut count = 0usize;
    for topic in targets {
        count = count.checked_add(1)?;
        charge = charge
            .checked_add(OWNED_ENTRY_CHARGE)?
            .checked_add(CORRELATION_ENTRY_CHARGE)?
            .checked_add(EXPECTED_SORT_ENTRY_CHARGE)?
            .checked_add(RESPONSE_SORT_ENTRY_CHARGE)?
            .checked_add(topic.len())?;
    }
    Some((count, charge))
}

#[cfg(test)]
pub(super) const MINIMUM_ENTRY_CHARGE: usize = OWNED_ENTRY_CHARGE
    + CORRELATION_ENTRY_CHARGE
    + EXPECTED_SORT_ENTRY_CHARGE
    + RESPONSE_SORT_ENTRY_CHARGE;

/// Conservatively charges the fully built generated request beside its sort
/// scratch. Treating every target as a distinct topic safely overcharges
/// repeated-topic batches without allocating a grouping index first.
pub(crate) fn generated_request_peak_charge<'a>(
    group_id: &str,
    mut targets: impl Iterator<Item = OffsetCommitTargetRef<'a>>,
) -> Option<usize> {
    targets.try_fold(
        size_of::<OffsetCommitRequest>()
            .checked_add(size_of::<Vec<usize>>())?
            .checked_add(group_id.len())?,
        |charge, target| {
            charge
                .checked_add(REQUEST_SORT_ENTRY_CHARGE)?
                .checked_add(GENERATED_TOPIC_CHARGE)?
                .checked_add(GENERATED_PARTITION_CHARGE)?
                .checked_add(target.topic().len())?
                .checked_add(target.metadata().map_or(0, str::len))
        },
    )
}
