//! Pre-core-admission reservation for exact normalized commit outcomes.

use kafka_client_core::GroupOffsetCommitPartitionOutcome;

use super::validation::MAX_GROUP_OFFSET_COMMIT_ENTRIES;

/// Why exact result capacity could not be reserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitResultReservationError {
    EntryCapacity { actual: usize, limit: usize },
    ReservationCapacity { required: usize, actual: usize },
    AllocationFailed,
}

/// Linear preallocated-capacity owner transferred into one prepared commit.
#[must_use = "result capacity must be transferred or deliberately released"]
#[derive(Debug)]
pub(crate) struct GroupOffsetCommitResultReservation {
    entry_count: usize,
    outcomes: Vec<GroupOffsetCommitPartitionOutcome>,
}

impl GroupOffsetCommitResultReservation {
    pub(crate) fn try_new(
        entry_count: usize,
    ) -> Result<Self, GroupOffsetCommitResultReservationError> {
        Self::reserve(entry_count, entry_count)
    }

    #[cfg(test)]
    pub(super) fn try_new_with_capacity_for_test(
        entry_count: usize,
        reserved_capacity: usize,
    ) -> Result<Self, GroupOffsetCommitResultReservationError> {
        Self::reserve(entry_count, reserved_capacity)
    }

    fn reserve(
        entry_count: usize,
        reserved_capacity: usize,
    ) -> Result<Self, GroupOffsetCommitResultReservationError> {
        if entry_count > MAX_GROUP_OFFSET_COMMIT_ENTRIES {
            return Err(GroupOffsetCommitResultReservationError::EntryCapacity {
                actual: entry_count,
                limit: MAX_GROUP_OFFSET_COMMIT_ENTRIES,
            });
        }
        if reserved_capacity < entry_count {
            return Err(
                GroupOffsetCommitResultReservationError::ReservationCapacity {
                    required: entry_count,
                    actual: reserved_capacity,
                },
            );
        }
        let mut outcomes = Vec::new();
        outcomes
            .try_reserve_exact(reserved_capacity)
            .map_err(|_| GroupOffsetCommitResultReservationError::AllocationFailed)?;
        Ok(Self {
            entry_count,
            outcomes,
        })
    }

    pub(crate) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub(crate) fn outcomes_capacity(&self) -> usize {
        self.outcomes.capacity()
    }

    #[cfg(test)]
    pub(super) fn outcomes_ptr_for_test(&self) -> *const GroupOffsetCommitPartitionOutcome {
        self.outcomes.as_ptr()
    }

    pub(super) fn into_outcomes(self) -> Vec<GroupOffsetCommitPartitionOutcome> {
        self.outcomes
    }
}
