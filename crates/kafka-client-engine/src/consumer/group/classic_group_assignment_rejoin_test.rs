//! Exact catalog revocation after core enters recoverable or fatal rejection phases.

use kafka_client_core::{
    ClassicBrokerError, ClassicGroupEffect, ClassicGroupInput, ClassicGroupPhase,
    LiveGroupAssignment, Moment,
};

use super::{
    classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind,
    classic_group_test_support,
};

#[test]
fn waiting_to_rejoin_authorizes_its_exact_effect_owned_revoke() {
    assert_revoke_allowed(27, ClassicGroupPhase::WaitingToRejoin);
}

#[test]
fn fatal_rejection_authorizes_its_exact_effect_owned_revoke() {
    assert_revoke_allowed(1234, ClassicGroupPhase::Fatal);
}

fn assert_revoke_allowed(error_code: i16, expected_phase: ClassicGroupPhase) {
    let group_id = kafka_client_core::GroupId::try_from_raw(31)
        .unwrap_or_else(|| panic!("nonzero group identity"));
    let mut catalog = super::session_catalog::GroupSessionCatalog::try_new(
        group_id,
        std::sync::Arc::from("workers"),
        &[std::sync::Arc::from("orders")],
    )
    .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let mut owner = super::classic_group_owner::ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let heartbeat = classic_group_test_support::install_follower(
        &mut catalog,
        &mut owner,
        "member-a",
        7,
        Vec::new(),
    );
    let attempt = heartbeat.attempt();
    owner
        .apply(ClassicGroupInput::HeartbeatDue {
            attempt,
            now: Moment::from_tick(heartbeat.next_deadline().tick()),
        })
        .unwrap_or_else(|error| panic!("heartbeat due failed: {error}"));
    let transition = owner
        .apply(ClassicGroupInput::HeartbeatRejected {
            attempt,
            now: Moment::from_tick(heartbeat.next_deadline().tick()),
            error: ClassicBrokerError::try_from_code(error_code)
                .unwrap_or_else(|| panic!("nonzero broker error")),
        })
        .unwrap_or_else(|error| panic!("heartbeat rejection failed: {error}"));
    let (assignment, classic_generation) = transition
        .into_effects()
        .find_map(|effect| match effect {
            ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            } => Some((assignment, classic_generation)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("Revoke effect expected"));

    assert_eq!(owner.machine().phase(), expected_phase);
    owner
        .prepare_revoke(&mut catalog, assignment, classic_generation)
        .unwrap_or_else(|failure| {
            panic!(
                "effect-owned revoke rejected in {expected_phase:?}: {:?}",
                failure.kind
            )
        })
        .commit();
    assert!(catalog.live_assignment().is_none());
}

#[test]
fn stable_phase_still_rejects_an_unauthorized_revoke_preparation() {
    let group_id = kafka_client_core::GroupId::try_from_raw(32)
        .unwrap_or_else(|| panic!("nonzero group identity"));
    let mut catalog = super::session_catalog::GroupSessionCatalog::try_new(
        group_id,
        std::sync::Arc::from("workers"),
        &[std::sync::Arc::from("orders")],
    )
    .unwrap_or_else(|error| panic!("catalog creation failed: {error:?}"));
    let mut owner = super::classic_group_owner::ClassicGroupOwner::new(
        group_id,
        classic_group_test_support::timing(),
        classic_group_test_support::heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    classic_group_test_support::install_follower(
        &mut catalog,
        &mut owner,
        "member-a",
        7,
        Vec::new(),
    );
    let current = catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment expected"));
    let assignment = LiveGroupAssignment::try_new(
        current.group_id(),
        current.member_id(),
        current.assignment_generation(),
        current.partitions().to_vec(),
    )
    .unwrap_or_else(|error| panic!("matching assignment failed: {error:?}"));
    let generation = owner
        .machine()
        .live_generation()
        .unwrap_or_else(|| panic!("live generation expected"));

    let failure = owner
        .prepare_revoke(&mut catalog, assignment, generation)
        .err()
        .unwrap_or_else(|| panic!("stable phase must reject revoke"));
    assert_eq!(
        failure.kind,
        ClassicGroupAssignmentPreparationFailureKind::MachinePhase
    );
}
