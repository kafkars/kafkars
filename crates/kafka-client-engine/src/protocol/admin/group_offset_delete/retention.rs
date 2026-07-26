//! Allocation-free retained-capacity proof for normalized offset-deletion results.

use core::{mem::size_of, num::NonZeroI16};

use super::{
    OffsetDeletePartitionRef,
    correlation::{BorrowedOffsetDeletePartition, IndexedOffsetDeleteTarget},
};

type OwnedOffsetDeleteEntryCharge = (String, i32, Result<(), NonZeroI16>);

const BASE_RESULT_CHARGE: usize = size_of::<Vec<OwnedOffsetDeleteEntryCharge>>()
    + size_of::<Vec<OffsetDeletePartitionRef<'static>>>()
    + size_of::<Vec<IndexedOffsetDeleteTarget<'static>>>()
    + size_of::<Vec<BorrowedOffsetDeletePartition<'static>>>()
    + size_of::<Option<NonZeroI16>>()
    + size_of::<u32>();
const OWNED_ENTRY_CHARGE: usize = size_of::<OwnedOffsetDeleteEntryCharge>();
const CORRELATION_ENTRY_CHARGE: usize = size_of::<OffsetDeletePartitionRef<'static>>();
const EXPECTED_SORT_ENTRY_CHARGE: usize = size_of::<IndexedOffsetDeleteTarget<'static>>();
const RESPONSE_SORT_ENTRY_CHARGE: usize = size_of::<BorrowedOffsetDeletePartition<'static>>();
const REQUEST_SORT_ENTRY_CHARGE: usize = size_of::<usize>();

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

pub(crate) fn request_grouping_charge(target_count: usize) -> Option<usize> {
    size_of::<Vec<usize>>().checked_add(target_count.checked_mul(REQUEST_SORT_ENTRY_CHARGE)?)
}
