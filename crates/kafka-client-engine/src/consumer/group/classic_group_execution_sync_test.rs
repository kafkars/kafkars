//! Exact Sync handoff, receipt fencing, and submission-failure scenarios.

use std::time::Instant;

use kafka_client_core::{ClassicGeneration, Deadline, GroupId, MemberId, MembershipCycle};

use crate::{
    clock::OperationDeadline,
    driver::classic_group::{AcceptedSyncGroupCall, SyncGroupCallKey},
    protocol::consumer::classic_follower_sync_group_request,
};

use super::{
    classic_group_execution::{
        ClassicGroupExecution, ClassicGroupExecutionError, new_classic_group_execution,
    },
    classic_group_join::ClassicGroupExecutionState,
    classic_group_sync::{
        ClassicGroupSyncAcceptanceFailure, ClassicGroupSyncDriverOwner, ClassicGroupSyncIdentity,
        PreparedClassicGroupSync,
    },
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
fn begin_moves_the_exact_prepared_sync_into_handoff() {
    let (mut execution, identity) = prepared_execution();
    assert_eq!(
        execution
            .prepared_sync()
            .map(PreparedClassicGroupSync::identity),
        Some(identity)
    );

    let prepared = execution
        .begin_sync_handoff()
        .unwrap_or_else(|error| panic!("Sync handoff failed: {error:?}"));
    let (returned_identity, request) = prepared.into_parts();

    assert_eq!(returned_identity, identity);
    assert_exact_handoff(&execution, identity);
    drop(request);
}

#[test]
fn changed_deadline_preserves_the_exact_handoff_and_both_receipts() {
    let (mut execution, identity) = handed_off_execution();
    let changed_deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(identity.deadline().core().tick() + 1),
        identity.deadline().transport(),
    );
    let supplied_key =
        SyncGroupCallKey::new(identity.group_id(), identity.cycle(), changed_deadline);
    let accepted = AcceptedSyncGroupCall::from_key_for_test(supplied_key);

    let failure = execution
        .confirm_sync_driver_owned(identity, accepted)
        .err()
        .unwrap_or_else(|| panic!("changed-deadline Sync receipt must be rejected"));

    assert_exact_handoff(&execution, identity);
    assert_eq!(failure.identity(), identity);
    let (returned_identity, returned_receipt) = failure.into_parts();
    assert_eq!(returned_identity, identity);
    assert_eq!(returned_receipt.key(), supplied_key);
}

#[test]
fn wrong_identity_preserves_the_exact_handoff_and_both_receipts() {
    let (mut execution, identity) = handed_off_execution();
    let wrong_member = MemberId::try_from_raw(identity.member_id().get() + 1)
        .unwrap_or_else(|| panic!("next member identity"));
    let supplied_identity = ClassicGroupSyncIdentity::new(
        identity.group_id(),
        identity.cycle(),
        wrong_member,
        identity.generation(),
        identity.deadline(),
    );
    let supplied_key =
        SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
    let accepted = AcceptedSyncGroupCall::from_key_for_test(supplied_key);

    let failure = execution
        .confirm_sync_driver_owned(supplied_identity, accepted)
        .err()
        .unwrap_or_else(|| panic!("wrong Sync identity must be rejected"));

    assert_exact_handoff(&execution, identity);
    let (returned_identity, returned_receipt) = failure.into_parts();
    assert_eq!(returned_identity, supplied_identity);
    assert_eq!(returned_receipt.key(), supplied_key);
}

#[test]
fn exact_receipt_moves_the_sync_into_driver_ownership() {
    let (mut execution, identity) = handed_off_execution();
    let key = SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
    let accepted = AcceptedSyncGroupCall::from_key_for_test(key);

    execution
        .confirm_sync_driver_owned(identity, accepted)
        .unwrap_or_else(|_failure| panic!("exact Sync receipt must be accepted"));

    assert!(matches!(
        execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncDriverOwned(owner)
            if owner.identity() == identity && owner.accepted().key() == key
    ));
}

#[test]
fn submission_failure_requires_the_exact_handoff_identity() {
    let (mut execution, identity) = handed_off_execution();
    let wrong_cycle = MembershipCycle::try_from_raw(identity.cycle().get() + 1)
        .unwrap_or_else(|| panic!("next membership cycle"));
    let wrong_identity = ClassicGroupSyncIdentity::new(
        identity.group_id(),
        wrong_cycle,
        identity.member_id(),
        identity.generation(),
        identity.deadline(),
    );

    assert_eq!(
        execution.finish_sync_submission_failure(wrong_identity),
        Err(ClassicGroupExecutionError::HandoffMismatch)
    );
    assert_exact_handoff(&execution, identity);
    assert_eq!(execution.finish_sync_submission_failure(identity), Ok(()));
    assert!(matches!(
        execution.borrow_execution_state(),
        ClassicGroupExecutionState::Idle
    ));
}

#[test]
fn sync_acceptance_failures_and_driver_owners_remain_linear() {
    assert_not_impl!(ClassicGroupSyncAcceptanceFailure: Clone);
    assert_not_impl!(ClassicGroupSyncAcceptanceFailure: Copy);
    assert_not_impl!(ClassicGroupSyncDriverOwner: Clone);
    assert_not_impl!(ClassicGroupSyncDriverOwner: Copy);
}

fn prepared_execution() -> (ClassicGroupExecution, ClassicGroupSyncIdentity) {
    let identity = identity();
    let request = classic_follower_sync_group_request("workers", "member-a", identity.generation())
        .unwrap_or_else(|error| panic!("follower Sync request failed: {error:?}"));
    let prepared = PreparedClassicGroupSync::new(identity, request);
    let mut execution = new_classic_group_execution();
    execution.set_execution_state(ClassicGroupExecutionState::PreparedSync(prepared));
    (execution, identity)
}

fn handed_off_execution() -> (ClassicGroupExecution, ClassicGroupSyncIdentity) {
    let (mut execution, identity) = prepared_execution();
    let prepared = execution
        .begin_sync_handoff()
        .unwrap_or_else(|error| panic!("Sync handoff failed: {error:?}"));
    drop(prepared);
    (execution, identity)
}

fn identity() -> ClassicGroupSyncIdentity {
    ClassicGroupSyncIdentity::new(
        GroupId::try_from_raw(7).unwrap_or_else(|| panic!("group identity")),
        MembershipCycle::try_from_raw(11).unwrap_or_else(|| panic!("membership cycle")),
        MemberId::try_from_raw(13).unwrap_or_else(|| panic!("member identity")),
        ClassicGeneration::try_from_raw(17).unwrap_or_else(|| panic!("classic generation")),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(23), Instant::now()),
    )
}

fn assert_exact_handoff(execution: &ClassicGroupExecution, expected: ClassicGroupSyncIdentity) {
    assert!(matches!(
        execution.borrow_execution_state(),
        ClassicGroupExecutionState::SyncHandoff(actual) if *actual == expected
    ));
}
