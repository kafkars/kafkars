//! Atomic follower Join staging and exact prepared-Sync ownership scenarios.

use std::{sync::Arc, time::Instant};

use kafka_client_core::{
    ClassicGeneration, ClassicGroupErrorKind, ClassicGroupPhase, Deadline, GroupId,
    MembershipCycle, Moment,
};

use crate::clock::OperationDeadline;

use super::{
    classic_group_candidate::ClassicGroupCycleCandidate,
    classic_group_owner::{ClassicGroupCandidateOwnershipError, ClassicGroupOwner},
    classic_group_owner_follower::ClassicGroupFollowerJoinError,
    classic_group_test_support,
    session_catalog::GroupSessionCatalog,
};

#[test]
fn follower_join_retains_candidate_and_prepares_the_exact_sync_fences() {
    let group_id = group_id();
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let catalog = catalog(group_id);
    let cycle = classic_group_test_support::begin(&mut owner);
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));
    let member_id = candidate.local_member_id();
    let generation = generation();
    let transport = Instant::now();
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), transport);

    let prepared = owner
        .apply_follower_join(
            catalog.group(),
            candidate,
            generation,
            Moment::from_tick(2),
            deadline,
        )
        .unwrap_or_else(|error| panic!("follower Join failed: {error:?}"));

    assert_eq!(prepared.group_id(), group_id);
    assert_eq!(prepared.cycle(), cycle);
    assert_eq!(prepared.member_id(), member_id);
    assert_eq!(prepared.generation(), generation);
    assert_eq!(prepared.deadline().core(), Deadline::from_tick(100));
    assert_eq!(prepared.deadline().transport(), transport);
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Syncing);
    assert_eq!(owner.machine().active_cycle(), Some(cycle));
    assert_eq!(owner.machine().deadline(), Some(Deadline::from_tick(100)));
    assert!(owner.machine().live_assignment().is_none());
    assert_eq!(
        owner
            .pending()
            .map(ClassicGroupCycleCandidate::local_member_id),
        Some(member_id)
    );
}

#[test]
fn prevalidation_rejection_preserves_the_joining_owner_exactly() {
    let group_id = group_id();
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let catalog = catalog(group_id);
    let active_cycle = classic_group_test_support::begin(&mut owner);
    let wrong_cycle = MembershipCycle::try_from_raw(active_cycle.get() + 1)
        .unwrap_or_else(|| panic!("next membership cycle"));
    let candidate = catalog
        .prepare_follower_cycle(wrong_cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now());

    let result = owner.apply_follower_join(
        catalog.group(),
        candidate,
        generation(),
        Moment::from_tick(2),
        deadline,
    );

    assert_eq!(
        result.err(),
        Some(ClassicGroupFollowerJoinError::Candidate(
            ClassicGroupCandidateOwnershipError::Cycle
        ))
    );
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Joining);
    assert_eq!(owner.machine().active_cycle(), Some(active_cycle));
    assert_eq!(owner.machine().deadline(), Some(Deadline::from_tick(100)));
    assert!(owner.pending().is_none());
}

#[test]
fn postvalidation_core_rejection_retains_the_exact_candidate() {
    let group_id = group_id();
    let mut owner = ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let catalog = catalog(group_id);
    let cycle = classic_group_test_support::begin(&mut owner);
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));
    let member_id = candidate.local_member_id();
    let deadline = OperationDeadline::from_parts_for_test(Deadline::from_tick(100), Instant::now());

    let result = owner.apply_follower_join(
        catalog.group(),
        candidate,
        generation(),
        Moment::from_tick(100),
        deadline,
    );

    assert_eq!(
        result.err(),
        Some(ClassicGroupFollowerJoinError::Core(
            ClassicGroupErrorKind::DeadlineElapsed
        ))
    );
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Joining);
    assert_eq!(owner.machine().active_cycle(), Some(cycle));
    assert_eq!(owner.machine().deadline(), Some(Deadline::from_tick(100)));
    assert_eq!(
        owner
            .pending()
            .map(ClassicGroupCycleCandidate::local_member_id),
        Some(member_id)
    );
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(7).unwrap_or_else(|| panic!("nonzero group identity"))
}

fn generation() -> ClassicGeneration {
    ClassicGeneration::try_from_raw(17).unwrap_or_else(|| panic!("nonnegative classic generation"))
}

fn catalog(group_id: GroupId) -> GroupSessionCatalog {
    GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
        .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"))
}
