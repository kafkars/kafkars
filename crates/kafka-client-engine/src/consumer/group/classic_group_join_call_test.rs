//! Exact Join handoff fencing and linear acceptance-failure ownership.

use std::time::Duration;

use kafka_client_core::{
    ClassicGroupTiming, ClassicHeartbeatPolicy, Deadline, GroupId, MembershipCycle,
};

use crate::{
    clock::{MonotonicClock, OperationDeadline},
    driver::classic_group::{AcceptedJoinGroupCall, JoinGroupCallKey},
};

use super::{
    classic_group_execution::{ClassicGroupExecution, new_classic_group_execution},
    classic_group_join::{
        ClassicGroupExecutionState, ClassicGroupJoinDriverAcceptance, ClassicGroupJoinIdentity,
    },
    classic_group_join_call::{ClassicGroupJoinAcceptanceFailure, ClassicGroupJoinCallOwner},
    classic_group_owner::ClassicGroupOwner,
    classic_group_test_support,
};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Implemented;
            trait AmbiguousIfImplemented<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
            impl<T: ?Sized + $trait> AmbiguousIfImplemented<Implemented> for T {}
            let _ = <$type as AmbiguousIfImplemented<_>>::check;
        };
    };
}

#[test]
fn wrong_group_and_cycle_preserve_the_exact_join_handoff_and_both_receipts() {
    let (mut execution, acceptance, identity) = prepared_acceptance();
    let wrong_group =
        GroupId::try_from_raw(identity.group_id().get() + 1).unwrap_or_else(|| panic!("group"));
    let wrong_cycle = MembershipCycle::try_from_raw(identity.cycle().get() + 1)
        .unwrap_or_else(|| panic!("cycle"));
    let supplied_key = JoinGroupCallKey::new(wrong_group, wrong_cycle, identity.deadline());
    let accepted = AcceptedJoinGroupCall::from_key_for_test(supplied_key);

    let failure = execution
        .confirm_join_driver_owned(acceptance, accepted)
        .err()
        .unwrap_or_else(|| panic!("mismatched Join receipt must be rejected"));

    assert_exact_handoff(&execution, identity);
    assert_eq!(failure.identity(), identity);
    let (returned_acceptance, returned_receipt) = failure.into_parts();
    assert_eq!(returned_acceptance.identity(), identity);
    assert_eq!(returned_receipt.key(), supplied_key);
}

#[test]
fn changed_deadline_preserves_the_exact_join_handoff_and_both_receipts() {
    let (mut execution, acceptance, identity) = prepared_acceptance();
    let changed_deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(identity.deadline().core().tick() + 1),
        identity.deadline().transport(),
    );
    let supplied_key =
        JoinGroupCallKey::new(identity.group_id(), identity.cycle(), changed_deadline);
    let accepted = AcceptedJoinGroupCall::from_key_for_test(supplied_key);

    let failure = execution
        .confirm_join_driver_owned(acceptance, accepted)
        .err()
        .unwrap_or_else(|| panic!("changed-deadline Join receipt must be rejected"));

    assert_exact_handoff(&execution, identity);
    assert_eq!(failure.identity().deadline(), identity.deadline());
    let (returned_acceptance, returned_receipt) = failure.into_parts();
    assert_eq!(returned_acceptance.identity(), identity);
    assert_eq!(returned_receipt.key().group_id(), identity.group_id());
    assert_eq!(returned_receipt.key().cycle(), identity.cycle());
    assert_eq!(returned_receipt.key().deadline(), changed_deadline);
}

#[test]
fn accepted_join_owners_and_failures_remain_linear() {
    assert_not_impl!(ClassicGroupJoinCallOwner: Clone);
    assert_not_impl!(ClassicGroupJoinCallOwner: Copy);
    assert_not_impl!(ClassicGroupJoinAcceptanceFailure: Clone);
    assert_not_impl!(ClassicGroupJoinAcceptanceFailure: Copy);
}

fn prepared_acceptance() -> (
    ClassicGroupExecution,
    ClassicGroupJoinDriverAcceptance,
    ClassicGroupJoinIdentity,
) {
    let group_id = GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group identity"));
    let timing = ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"));
    let heartbeat = ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"));
    let mut owner = ClassicGroupOwner::new(
        group_id,
        timing,
        heartbeat,
        classic_group_test_support::rejoin_policy(),
    );
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(7))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let handoff = execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));
    let identity = handoff.identity();
    (execution, handoff.into_driver_acceptance(), identity)
}

fn assert_exact_handoff(execution: &ClassicGroupExecution, expected: ClassicGroupJoinIdentity) {
    assert!(matches!(
        execution.borrow_execution_state(),
        ClassicGroupExecutionState::JoinHandoff(actual) if *actual == expected
    ));
}
