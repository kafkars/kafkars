//! Stable exact-broker grouping scenarios for borrow-only route plans.

use super::resolution::{first_available_broker_group, plan_groups};

#[test]
fn interleaved_same_broker_candidates_share_one_stable_group() {
    let plans =
        plan_groups(&[3, 7, 3, 11, 7]).unwrap_or_else(|| panic!("small route plan must fit"));

    assert_eq!(plans, [(3, 2), (7, 2), (11, 1)]);
}

#[test]
fn saturated_first_broker_does_not_block_a_later_available_group() {
    let selected = first_available_broker_group([3, 7, 11], |broker_id| broker_id != 3);

    assert_eq!(selected, Some(1));
}
