//! Exact-broker permit construction and retained-lane identity evidence.

use super::TrackedProduceCalls;

#[test]
fn reserved_permit_carries_only_the_selected_exact_broker_lane() {
    let mut calls = TrackedProduceCalls::new(1);
    let permit = calls
        .try_reserve_for(17)
        .unwrap_or_else(|| panic!("one exact-broker lane should be available"));

    assert_eq!(permit.reserved_exact_broker_id(), 17);
    drop(permit);
    assert_eq!(calls.retained_count(), 0);
}
