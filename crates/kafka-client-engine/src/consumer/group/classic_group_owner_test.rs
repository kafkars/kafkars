//! Per-entry deterministic classic-group ownership scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicBrokerError, ClassicGroupInput, ClassicGroupPhase, Deadline, GroupId, Moment,
};

use super::{
    classic_group_test_support, registry_entry::GroupConsumerEntry,
    session_catalog::GroupSessionCatalog,
};

#[test]
fn entry_owns_one_dormant_machine_for_its_exact_group() {
    let group_id =
        GroupId::try_from_raw(17).unwrap_or_else(|| panic!("group identity must be nonzero"));
    let entry = GroupConsumerEntry::try_new(
        group_id,
        &Arc::from("workers"),
        &[Arc::from("orders")],
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    )
    .unwrap_or_else(|error| panic!("entry creation failed: {error:?}"));

    assert_eq!(entry.classic.machine().group_id(), group_id);
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Dormant);
    assert!(entry.classic.is_dormant());
}

#[test]
fn recoverable_rejection_clears_the_prior_cycle_candidate() {
    let (mut owner, catalog) = joining_owner();
    let cycle = owner
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));
    owner
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("candidate stage failed: {error:?}"));

    owner
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(10),
            error: broker_error(14),
        })
        .unwrap_or_else(|error| panic!("recoverable rejection failed: {error}"));

    assert_eq!(owner.machine().phase(), ClassicGroupPhase::WaitingToRejoin);
    assert!(owner.pending().is_none());
}

#[test]
fn fatal_rejection_clears_the_prior_cycle_candidate() {
    let (mut owner, catalog) = joining_owner();
    let cycle = owner
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active cycle expected"));
    let candidate = catalog
        .prepare_follower_cycle(cycle, Arc::from("member-a"))
        .unwrap_or_else(|error| panic!("candidate preparation failed: {error:?}"));
    owner
        .stage_candidate(candidate)
        .unwrap_or_else(|error| panic!("candidate stage failed: {error:?}"));

    owner
        .apply(ClassicGroupInput::JoinRejected {
            cycle,
            now: Moment::from_tick(10),
            error: broker_error(79),
        })
        .unwrap_or_else(|error| panic!("fatal rejection failed: {error}"));

    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Fatal);
    assert!(owner.pending().is_none());
}

fn joining_owner() -> (
    super::classic_group_owner::ClassicGroupOwner,
    GroupSessionCatalog,
) {
    let group_id = GroupId::try_from_raw(23).unwrap_or_else(|| panic!("nonzero group identity"));
    let mut owner = super::classic_group_owner::ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    owner
        .apply(ClassicGroupInput::Begin {
            now: Moment::from_tick(1),
            deadline: Deadline::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("begin failed: {error}"));
    let catalog =
        GroupSessionCatalog::try_new(group_id, Arc::from("workers"), &[Arc::from("orders")])
            .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    (owner, catalog)
}

fn broker_error(code: i16) -> ClassicBrokerError {
    ClassicBrokerError::try_from_code(code).unwrap_or_else(|| panic!("nonzero broker error"))
}
