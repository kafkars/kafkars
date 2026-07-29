//! Explicit response-terminal and diagnostic retention bounds.

use core::mem::size_of;

use super::NormalizedRemoveRaftVoterResponse;

pub(crate) const REMOVE_RAFT_VOTER_MAX_RETAINED_BYTES: usize = 4 * 1_024;
pub(super) const REMOVE_RAFT_VOTER_MAX_DIAGNOSTIC_BYTES: usize = 1_024;

pub(super) fn bounded_diagnostic(source: Option<&str>) -> (Option<&str>, bool) {
    let Some(source) = source else {
        return (None, false);
    };
    let mut end = source.len().min(REMOVE_RAFT_VOTER_MAX_DIAGNOSTIC_BYTES);
    while !source.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    (Some(&source[..end]), end < source.len())
}

pub(super) fn retained_charge(diagnostic_capacity: usize) -> Option<usize> {
    size_of::<NormalizedRemoveRaftVoterResponse>().checked_add(diagnostic_capacity)
}
