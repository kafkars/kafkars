//! Registry staging, event observation, and terminal retirement scenarios.

use kafka_client_core::{
    AssignmentEpoch, ClassicGroupEffect, ClassicGroupInput, Deadline, GroupId, Moment,
};

use crate::consumer::GroupConsumerEvent;

use super::{
    classic_group_graceful_revocation::ClassicGroupRevocationTurn,
    registry::GroupConsumerRegistry,
    registry_graceful_revocation::stage_classic_group_revocation,
    registry_test_support::{
        install_ready_group_delivery, install_session, register, started_registry,
    },
};

#[test]
fn observed_ack_retains_terminal_until_exact_assignment_retirement() {
    let deadline = Deadline::from_tick(90);
    let (mut registry, group_id, epoch) = staged_registry(deadline);

    let event = registry
        .take_event(group_id)
        .unwrap_or_else(|error| panic!("event observation: {error:?}"))
        .unwrap_or_else(|| panic!("one revocation event expected"));
    let GroupConsumerEvent::PartitionsRevoked(assignment) = event else {
        panic!("revocation event expected");
    };
    assert_eq!(assignment.assignment_epoch(), epoch.get());
    registry
        .acknowledge_revocation(group_id, epoch.get(), Moment::from_tick(89))
        .unwrap_or_else(|error| panic!("completion: {error:?}"));

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.catalog.live_assignment().is_some());
    assert!(entry.revocation.terminal().is_some());
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(89)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_retired(&registry, group_id);
}

#[test]
fn exact_deadline_loses_then_retires_on_the_next_bounded_turn() {
    let deadline = Deadline::from_tick(90);
    let (mut registry, group_id, _epoch) = staged_registry(deadline);

    assert_eq!(registry.graceful_revocation_next_deadline(), Some(deadline));
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(90)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.catalog.live_assignment())
            .is_some()
    );
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(90)),
        Ok(ClassicGroupRevocationTurn::Progress)
    );
    assert_retired(&registry, group_id);
}

fn staged_registry(deadline: Deadline) -> (GroupConsumerRegistry, GroupId, AssignmentEpoch) {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    let (assignment, generation, epoch) = {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("registered entry expected"));
        entry.catalog.stage_installed_assignment_event();
        entry.catalog.confirm_sync_event();
        let epoch = entry
            .fetch
            .activation()
            .unwrap_or_else(|| panic!("Fetch activation expected"))
            .binding()
            .assignment_epoch();
        let cycle = entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active cycle expected"));
        let effect = entry
            .classic
            .apply(ClassicGroupInput::AssignmentLost { cycle })
            .unwrap_or_else(|error| panic!("assignment loss: {error}"))
            .into_effects()
            .next()
            .unwrap_or_else(|| panic!("Revoke effect expected"));
        let ClassicGroupEffect::Revoke {
            assignment,
            classic_generation,
        } = effect
        else {
            panic!("Revoke effect expected");
        };
        (assignment, classic_generation, epoch)
    };
    {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("registered entry expected"));
        stage_classic_group_revocation(
            &mut entry.catalog,
            &entry.fetch,
            &mut entry.revocation,
            assignment,
            generation,
            deadline,
            Moment::from_tick(50),
        )
        .unwrap_or_else(|(error, _assignment)| panic!("staging: {error:?}"));
    }
    (registry, group_id, epoch)
}

fn assert_retired(registry: &GroupConsumerRegistry, group_id: GroupId) {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.fetch.machine_assignment_epoch().is_none());
    assert!(entry.revocation.is_dormant());
    assert_eq!(registry.graceful_revocation_unsettled(), 0);
}
