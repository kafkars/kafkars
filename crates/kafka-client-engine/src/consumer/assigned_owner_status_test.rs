//! Scheduling-interest evidence for one concrete assigned-consumer lifecycle.

use super::assigned_owner_test::{driver, owner};

#[test]
fn idle_open_owner_is_settled_but_not_closed() {
    let owner = owner(1);

    assert_eq!(owner.unsettled(), 0);
    assert!(!owner.close_completed());
}

#[test]
fn accepted_close_remains_unsettled_until_core_terminal_retention() {
    let mut owner = owner(1);
    let _observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));

    assert!(owner.unsettled() > 0);
    assert!(!owner.close_completed());
}

#[test]
fn core_completion_remains_unsettled_until_notifier_publication() {
    let mut owner = owner(1);
    let _observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    let driver = driver();

    for _attempt in 0..3 {
        let _turn = owner.turn(&driver);
    }

    assert!(owner.close_completed());
    assert_eq!(owner.unsettled(), 1);

    let _publication_turn = owner.turn(&driver);

    assert_eq!(owner.unsettled(), 0);
}
