//! Admission fencing rechecks stale-open callers inside the owner lock.

use super::shard_test::setup;

#[test]
fn close_winning_the_owner_lock_rejects_a_stale_open_admission() {
    let (_owner, port, _wake) = setup();
    assert!(!port.shared.assigned_admission_is_closed());

    let _accepted = port
        .begin_close()
        .unwrap_or_else(|error| panic!("close must commit: {error:?}"));
    let mut operation_ran = false;
    let admitted = port
        .shared
        .try_admit_with_owner(|_owner| operation_ran = true)
        .unwrap_or_else(|error| panic!("owner lock remains available: {error:?}"));

    assert!(admitted.is_none());
    assert!(!operation_ran);
}
