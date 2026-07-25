//! Exact owner fixture for group commit call and settlement fields.

struct TrackedGroupOffsetCommitCalls {
    calls: usize,
    settled: usize,
    pending_confirmation: usize,
    completion_failure: usize,
}

fn own(calls: &mut TrackedGroupOffsetCommitCalls) {
    calls.calls += 1;
    calls.settled += 1;
    calls.pending_confirmation += 1;
    calls.completion_failure += 1;
}
