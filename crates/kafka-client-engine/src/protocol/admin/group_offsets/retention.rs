//! Allocation-free retained-capacity proof for normalized group offsets.

use core::{mem::size_of, num::NonZeroI16};

use super::model::BorrowedGroupOffset;

type OwnedGroupOffsetCharge = (String, i32, OwnedGroupOffsetValueCharge);
pub(super) type OwnedGroupOffsetValueCharge =
    Result<(Option<i64>, Option<i32>, Option<String>), NonZeroI16>;

const BASE_RESULT_CHARGE: usize = size_of::<Vec<OwnedGroupOffsetCharge>>()
    + size_of::<Vec<BorrowedGroupOffset<'static>>>()
    + size_of::<Option<NonZeroI16>>()
    + size_of::<u32>();
const OWNED_ENTRY_CHARGE: usize = size_of::<OwnedGroupOffsetCharge>();
const SORT_ENTRY_CHARGE: usize = size_of::<BorrowedGroupOffset<'static>>();

pub(super) fn validated_result_charge<'a>(
    entries: impl Iterator<Item = (&'a str, i16, Option<&'a str>)>,
) -> Option<(usize, usize)> {
    let mut charge = BASE_RESULT_CHARGE;
    let mut count = 0usize;
    for (topic, error_code, metadata) in entries {
        count = count.checked_add(1)?;
        charge = charge
            .checked_add(OWNED_ENTRY_CHARGE)?
            .checked_add(SORT_ENTRY_CHARGE)?
            .checked_add(topic.len())?;
        if error_code == 0 {
            charge = charge.checked_add(metadata.map_or(0, str::len))?;
        }
    }
    Some((count, charge))
}

#[cfg(test)]
pub(super) const MINIMUM_ENTRY_CHARGE: usize = OWNED_ENTRY_CHARGE + SORT_ENTRY_CHARGE;
