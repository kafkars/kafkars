//! Reviewed registry mutation owner fixture.

struct TrackedGroupPositionOffsetFetchCalls {
    calls: usize,
    settled: usize,
    pending_confirmation: usize,
    completion_failure: usize,
}

fn own(calls: &mut TrackedGroupPositionOffsetFetchCalls) {
    calls.calls += 1;
    calls.settled += 1;
    calls.pending_confirmation += 1;
    calls.completion_failure += 1;
}
