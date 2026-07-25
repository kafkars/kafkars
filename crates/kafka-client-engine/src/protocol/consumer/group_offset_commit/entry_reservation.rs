//! Pre-core-admission reservation for one prepared commit's entry storage.

use std::mem::size_of;

use super::{model::PreparedGroupOffsetCommitEntry, validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES};

/// Why exact prepared-entry capacity could not be reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitEntryReservationError {
    EntryCapacity { actual: usize, limit: usize },
    ReservationCapacity { required: usize, actual: usize },
    AllocationFailed,
}

/// Linear preallocated entry storage transferred into one prepared commit.
#[must_use = "entry capacity must be transferred or deliberately released"]
#[derive(Debug)]
pub(crate) struct GroupOffsetCommitEntryReservation {
    entry_count: usize,
    entries: Vec<PreparedGroupOffsetCommitEntry>,
}

impl GroupOffsetCommitEntryReservation {
    pub(crate) fn try_new(
        entry_count: usize,
    ) -> Result<Self, GroupOffsetCommitEntryReservationError> {
        Self::reserve(entry_count, entry_count)
    }

    #[cfg(test)]
    pub(super) fn try_new_with_capacity_for_test(
        entry_count: usize,
        reserved_capacity: usize,
    ) -> Result<Self, GroupOffsetCommitEntryReservationError> {
        Self::reserve(entry_count, reserved_capacity)
    }

    fn reserve(
        entry_count: usize,
        reserved_capacity: usize,
    ) -> Result<Self, GroupOffsetCommitEntryReservationError> {
        if entry_count > MAX_GROUP_OFFSET_COMMIT_ENTRIES {
            return Err(GroupOffsetCommitEntryReservationError::EntryCapacity {
                actual: entry_count,
                limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
            });
        }
        if reserved_capacity < entry_count {
            return Err(
                GroupOffsetCommitEntryReservationError::ReservationCapacity {
                    required: entry_count,
                    actual: reserved_capacity,
                },
            );
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(reserved_capacity)
            .map_err(|_| GroupOffsetCommitEntryReservationError::AllocationFailed)?;
        Ok(Self {
            entry_count,
            entries,
        })
    }

    pub(crate) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub(crate) fn entries_capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub(crate) fn reserved_bytes(&self) -> Option<usize> {
        self.entries
            .capacity()
            .checked_mul(size_of::<PreparedGroupOffsetCommitEntry>())
    }

    #[cfg(test)]
    pub(super) fn entries_ptr_for_test(&self) -> *const PreparedGroupOffsetCommitEntry {
        self.entries.as_ptr()
    }

    pub(super) fn into_entries(self) -> Vec<PreparedGroupOffsetCommitEntry> {
        self.entries
    }

    pub(super) fn recover_group_offset_commit_entries(
        entry_count: usize,
        mut entries: Vec<PreparedGroupOffsetCommitEntry>,
    ) -> Self {
        entries.clear();
        Self {
            entry_count,
            entries,
        }
    }
}
