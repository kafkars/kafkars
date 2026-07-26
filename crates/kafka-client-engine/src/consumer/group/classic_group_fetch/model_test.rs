//! First-slice private bounds and empty-owner construction scenarios.

use super::{
    ClassicGroupFetchOwner,
    owner::{
        FIRST_GROUP_FETCH_CALLS, FIRST_GROUP_FETCH_DELIVERIES, FIRST_GROUP_FETCH_DELIVERY_BYTES,
        FIRST_GROUP_FETCH_EFFECTS, FIRST_GROUP_FETCH_OUTPUT_BYTES, FIRST_GROUP_FETCH_PARTITIONS,
    },
};

#[test]
fn first_slice_reserves_every_pre_core_fifo_and_event_owner() {
    let owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));

    assert_eq!(FIRST_GROUP_FETCH_PARTITIONS, 64);
    assert_eq!(FIRST_GROUP_FETCH_EFFECTS, 129);
    assert_eq!(FIRST_GROUP_FETCH_CALLS, 1);
    assert_eq!(FIRST_GROUP_FETCH_DELIVERIES, 1);
    assert_eq!(FIRST_GROUP_FETCH_DELIVERY_BYTES, 1024 * 1024);
    assert_eq!(FIRST_GROUP_FETCH_OUTPUT_BYTES, 1024 * 1024);
    assert!(owner.effects.capacity() >= FIRST_GROUP_FETCH_EFFECTS);
    assert!(owner.pending_fetches.capacity() >= FIRST_GROUP_FETCH_PARTITIONS);
    assert_eq!(owner.events.retained(), (0, 0));
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert_eq!(owner.effect_count_for_test(), 0);
    assert_eq!(owner.pending_count_for_test(), 0);
    assert_eq!(owner.timer_count_for_test(), 0);
    assert_eq!(owner.next_deadline(), None);
}
