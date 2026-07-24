//! Scheduling-interest evidence for one concrete assigned-consumer lifecycle.

use super::assigned_owner_test::owner;

#[test]
fn idle_open_owner_is_settled_but_not_closed() {
    let owner = owner(1);

    assert_eq!(owner.unsettled(), 0);
    assert!(!owner.close_completed());
}

#[test]
fn accepted_close_remains_unsettled_until_core_terminal_retention() {
    let mut owner = owner(1);
    owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));

    assert!(owner.unsettled() > 0);
    assert!(!owner.close_completed());
}
