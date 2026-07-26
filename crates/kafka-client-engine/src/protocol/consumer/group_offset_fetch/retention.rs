//! Allocation-free charge for one normalized assigned-offset result.

use core::mem::size_of;

use super::model::GroupOffsetFetchPartitionValueRef;

const BASE_RESULT_CHARGE: usize = size_of::<Vec<GroupOffsetFetchPartitionValueRef<'static>>>()
    + size_of::<u32>()
    + size_of::<Option<core::num::NonZeroI16>>();
const ENTRY_CHARGE: usize = size_of::<GroupOffsetFetchPartitionValueRef<'static>>();

pub(super) fn normalized_result_charge<'a>(
    entries: impl Iterator<Item = (i16, Option<&'a str>)>,
) -> Option<(usize, usize)> {
    let mut count = 0usize;
    let mut charge = BASE_RESULT_CHARGE;
    for (error_code, metadata) in entries {
        count = count.checked_add(1)?;
        charge = charge.checked_add(ENTRY_CHARGE)?;
        if error_code == 0 {
            charge = charge.checked_add(metadata.map_or(0, str::len))?;
        }
    }
    Some((count, charge))
}

#[cfg(test)]
pub(super) const MINIMUM_ENTRY_CHARGE: usize = ENTRY_CHARGE;
