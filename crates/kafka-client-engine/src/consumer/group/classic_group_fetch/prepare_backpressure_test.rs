//! Prepared-Fetch FIFO saturation and release scenarios.

use kafka_client_core::Moment;

use crate::clock::MonotonicClock;

use super::{
    ClassicGroupFetchFront, ClassicGroupFetchOwner,
    test_support::{catalog, committed, completed_ready, position_fence},
};

#[test]
fn prepared_saturation_blocks_without_fault_then_same_head_proceeds_once() {
    let catalog = catalog(&["orders"]);
    let fence = position_fence(7);
    let mut owner =
        ClassicGroupFetchOwner::try_new().unwrap_or_else(|error| panic!("Fetch owner: {error:?}"));
    owner.partition_capacity = 1;
    owner
        .try_activate(
            completed_ready(fence, Moment::from_tick(41), 0, vec![committed(1, 1, 17)]),
            fence,
        )
        .unwrap_or_else(|error| panic!("Fetch activation: {:?}", error.kind()));
    let repeated = owner
        .front_effect_for_test()
        .unwrap_or_else(|| panic!("FetchReady retained"));
    let clock = MonotonicClock::new();
    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );
    owner.effects.push_back(repeated);

    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Backpressured
    );
    assert_eq!(owner.front_effect_for_test(), Some(repeated));
    assert!(owner.fault().is_none());
    let _submitted = owner
        .pop_prepared_for_test()
        .unwrap_or_else(|| panic!("one prepared slot released"));
    assert_eq!(
        owner.interpret_front_effect(&catalog, &clock),
        ClassicGroupFetchFront::Interpreted
    );
    assert_eq!(owner.effect_count_for_test(), 0);
    assert_eq!(owner.pending_count_for_test(), 1);
    assert!(owner.fault().is_none());
}
