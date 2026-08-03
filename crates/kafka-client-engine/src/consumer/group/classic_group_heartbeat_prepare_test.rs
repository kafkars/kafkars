//! Heartbeat cadence, liveness, and cooperative-reconciliation loss scenarios.

use kafka_client_core::{
    ClassicGroupInput, ClassicGroupPhase, ClassicProcessingLeaseError, ClassicProcessingLeaseFence,
    GroupId, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact, Moment,
    NextFetchOffset,
};

use crate::{clock::MonotonicClock, consumer::GroupConsumerEvent};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_fetch::ClassicGroupFetchFront,
    classic_group_graceful_revocation::ClassicGroupRevocationTurn,
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_prepare::{ClassicHeartbeatPreparationTurn, commit_local_loss},
    classic_group_position::test_support::completed_ready,
    classic_group_reconciliation_loss::ClassicGroupReconciliationLossTurn,
    classic_group_reconciliation_turn::ClassicGroupReconciliationTurn,
    registry::GroupConsumerRegistry,
    registry_event_reconciliation_test::prepared_reconciliation,
    registry_test_support::{install_session, register, started_registry, stop_registry},
};

#[test]
fn late_host_turn_revokes_instead_of_claiming_liveness() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let clock = MonotonicClock::new();

    assert_eq!(
        registry.prepare_one_classic_heartbeat(
            Moment::from_tick(schedule.liveness_deadline().tick()),
            &clock,
        ),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    stop_registry(&mut registry);
}

#[test]
fn prepared_attempt_uses_the_core_deadline_mapped_by_the_shared_epoch() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let clock = MonotonicClock::new();
    let now = Moment::from_tick(schedule.due().tick());

    assert_eq!(
        registry.prepare_one_classic_heartbeat(now, &clock),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );
    let prepared = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .unwrap_or_else(|| panic!("prepared Heartbeat expected"));
    let expected_core = now
        .checked_deadline_after(
            super::classic_group_test_support::heartbeat_policy().attempt_timeout_ticks(),
        )
        .map_or_else(
            || panic!("test Heartbeat deadline must fit"),
            |deadline| deadline.min(schedule.liveness_deadline()),
        );
    let expected = clock
        .operation_deadline(expected_core)
        .unwrap_or_else(|error| panic!("exact deadline mapping failed: {error}"));

    assert_eq!(prepared.key().deadline(), expected);
    stop_registry(&mut registry);
}

#[test]
fn locally_blocked_prepared_attempt_expires_and_revokes() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = schedule(&registry, group_id);
    let clock = MonotonicClock::new();
    registry
        .prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock)
        .unwrap_or_else(|error| panic!("Heartbeat preparation failed: {error:?}"));
    let deadline = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .map_or_else(
            || panic!("prepared deadline expected"),
            |prepared| prepared.key().deadline().core(),
        );

    assert_eq!(
        registry.expire_one_prepared_heartbeat(Moment::from_tick(deadline.tick())),
        Ok(true)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.heartbeat.is_dormant());
    stop_registry(&mut registry);
}

#[test]
fn cooperative_reconciliation_heartbeat_deadline_retires_the_previous_assignment() {
    let (mut registry, group_id, schedule) = staged_reconciliation_registry();
    let clock = MonotonicClock::new();

    assert_eq!(
        registry.prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );
    let deadline = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .map_or_else(
            || panic!("prepared replacement heartbeat"),
            |prepared| prepared.key().deadline().core(),
        );
    assert_eq!(
        registry.expire_one_prepared_heartbeat(Moment::from_tick(deadline.tick())),
        Ok(true),
        "replacement loss is staged without catalog AssignmentMismatch"
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("cooperative entry"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_some());
    assert!(
        entry
            .classic_reconciliation
            .as_ref()
            .is_some_and(|pending| pending.assignment_loss_is_staged())
    );
    assert!(entry.heartbeat.is_dormant());

    drain_reconciliation_loss(&mut registry);
    assert_reconciliation_loss_retired(&mut registry, group_id);
    let Some(GroupConsumerEvent::PartitionsLost(lost)) = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .and_then(|entry| entry.catalog.take_event())
    else {
        panic!("full previous assignment loss");
    };
    assert_eq!(lost.assignment_epoch(), 1);
    assert_eq!(lost.partitions().len(), 2);
    assert_eq!(lost.partitions()[0].partition(), 0);
    assert_eq!(lost.partitions()[1].partition(), 1);
    stop_registry(&mut registry);
}

#[test]
fn observed_partial_revocation_is_followed_by_full_loss_on_heartbeat_failure() {
    let (mut registry, group_id, schedule) = staged_reconciliation_registry();
    let Some(GroupConsumerEvent::PartitionsRevoked(revoked)) = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .and_then(|entry| entry.catalog.take_event())
    else {
        panic!("cooperative partial revocation");
    };
    assert_eq!(revoked.assignment_epoch(), 1);
    assert_eq!(revoked.partitions().len(), 1);
    assert_eq!(revoked.partitions()[0].partition(), 1);

    let clock = MonotonicClock::new();
    assert_eq!(
        registry.prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("cooperative entry"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatFailed {
            attempt: schedule.attempt(),
        })
        .unwrap_or_else(|error| panic!("replacement heartbeat failure: {error}"));
    commit_local_loss(entry, transition)
        .unwrap_or_else(|error| panic!("stage reconciliation loss: {error:?}"));
    entry
        .heartbeat
        .clear_local()
        .unwrap_or_else(|error| panic!("clear failed heartbeat: {error:?}"));

    drain_reconciliation_loss(&mut registry);
    assert_reconciliation_loss_retired(&mut registry, group_id);
    let Some(GroupConsumerEvent::PartitionsLost(lost)) = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .and_then(|entry| entry.catalog.take_event())
    else {
        panic!("full loss follows the observed subset revocation");
    };
    assert_eq!(lost.assignment_epoch(), 1);
    assert_eq!(lost.partitions().len(), 2);
    assert_eq!(lost.partitions()[0].partition(), 0);
    assert_eq!(lost.partitions()[1].partition(), 1);
    stop_registry(&mut registry);
}

#[test]
fn pre_stage_heartbeat_loss_drains_the_embedded_position_owner() {
    let (mut registry, group_id, schedule) = reconciliation_registry(false);
    let clock = MonotonicClock::new();
    assert_eq!(
        registry.prepare_one_classic_heartbeat(Moment::from_tick(schedule.due().tick()), &clock),
        Ok(ClassicHeartbeatPreparationTurn::Progress)
    );
    let deadline = registry
        .entry(group_id)
        .and_then(|entry| entry.heartbeat.prepared())
        .map_or_else(
            || panic!("prepared replacement heartbeat"),
            |prepared| prepared.key().deadline().core(),
        );
    assert_eq!(
        registry.expire_one_prepared_heartbeat(Moment::from_tick(deadline.tick())),
        Ok(true)
    );

    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(30)),
        Ok(ClassicGroupReconciliationLossTurn::Progress),
        "embedded position transfers before retirement"
    );
    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(31)),
        Ok(ClassicGroupReconciliationLossTurn::Progress),
        "transferred position closes through the ordinary owner"
    );

    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("cooperative entry"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("previous catalog assignment"));
    let previous_cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("previous catalog cycle"));
    let correct_fence = ClassicProcessingLeaseFence::new(
        group_id,
        previous_cycle,
        assignment.assignment_generation(),
    );
    entry
        .processing_lease
        .prepare_revocation(correct_fence)
        .unwrap_or_else(|error| panic!("release previous processing lease: {error:?}"))
        .commit();
    let wrong_fence = ClassicProcessingLeaseFence::new(
        group_id,
        previous_cycle
            .checked_next()
            .unwrap_or_else(|| panic!("alternate cycle")),
        assignment.assignment_generation(),
    );
    entry
        .processing_lease
        .prepare_activation(wrong_fence, Moment::from_tick(32))
        .unwrap_or_else(|error| panic!("install mismatched processing lease: {error:?}"))
        .commit();
    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(32)),
        Err(ClassicGroupExecutionError::ProcessingLease(
            ClassicProcessingLeaseError::FenceMismatch,
        )),
        "a retryable retirement mismatch retains every exact owner"
    );
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("retained cooperative entry"));
    assert!(entry.fault.is_none());
    assert!(entry.classic_reconciliation.is_some());
    assert!(entry.catalog.live_assignment().is_some());
    entry
        .processing_lease
        .prepare_revocation(wrong_fence)
        .unwrap_or_else(|error| panic!("release mismatched processing lease: {error:?}"))
        .commit();
    entry
        .processing_lease
        .prepare_activation(correct_fence, Moment::from_tick(33))
        .unwrap_or_else(|error| panic!("restore previous processing lease: {error:?}"))
        .commit();
    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(33)),
        Ok(ClassicGroupReconciliationLossTurn::Progress),
        "the retained loss owner retries exact retirement"
    );
    assert_reconciliation_loss_retired(&mut registry, group_id);
    stop_registry(&mut registry);
}

fn staged_reconciliation_registry() -> (
    GroupConsumerRegistry,
    GroupId,
    kafka_client_core::ClassicHeartbeatSchedule,
) {
    reconciliation_registry(true)
}

fn reconciliation_registry(
    stage: bool,
) -> (
    GroupConsumerRegistry,
    GroupId,
    kafka_client_core::ClassicHeartbeatSchedule,
) {
    let mut entry = prepared_reconciliation();
    let group_id = entry.group_id();
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("previous catalog assignment"));
    let cycle = entry
        .catalog
        .membership_cycle()
        .unwrap_or_else(|| panic!("previous catalog cycle"));
    let position_fence = GroupPositionFence::new(
        group_id,
        cycle,
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    entry
        .processing_lease
        .prepare_activation(
            ClassicProcessingLeaseFence::new(group_id, cycle, assignment.assignment_generation()),
            Moment::from_tick(10),
        )
        .unwrap_or_else(|error| panic!("previous processing lease: {error:?}"))
        .commit();
    let facts = assignment
        .partitions()
        .iter()
        .copied()
        .map(|partition| {
            GroupPositionPartitionFact::committed(
                partition,
                NextFetchOffset::try_from_raw(17).unwrap_or_else(|| panic!("positive next offset")),
            )
        })
        .collect();
    entry
        .fetch
        .try_activate(
            completed_ready(
                position_fence,
                Moment::from_tick(11),
                GroupPositionBatch::new(0, facts),
            ),
            position_fence,
        )
        .unwrap_or_else(|_error| panic!("previous Fetch activation"));
    let fetch_clock = MonotonicClock::new();
    for _effect in 0..assignment.partitions().len() {
        assert_eq!(
            entry
                .fetch
                .interpret_front_effect(&entry.catalog, &fetch_clock),
            ClassicGroupFetchFront::Interpreted,
            "previous Fetch activation effects drain before cooperative fencing"
        );
    }
    assert_eq!(
        entry
            .fetch
            .interpret_front_effect(&entry.catalog, &fetch_clock),
        ClassicGroupFetchFront::Idle
    );
    let pending = entry
        .classic_reconciliation
        .as_mut()
        .unwrap_or_else(|| panic!("pending cooperative reconciliation"));
    pending.confirm_sync();
    let schedule = pending.reconciliation().heartbeat();
    entry
        .heartbeat
        .prepare_install(schedule)
        .unwrap_or_else(|error| panic!("replacement heartbeat: {error:?}"))
        .commit();

    let retained_bytes = entry.group_bytes();
    let mut registry = GroupConsumerRegistry::start()
        .unwrap_or_else(|error| panic!("cooperative registry: {error}"));
    registry.retained_group_bytes = retained_bytes;
    registry.next_group_id = GroupId::try_from_raw(2);
    registry.entries.push(entry);
    if stage {
        assert_eq!(
            registry.stage_one_classic_group_reconciliation(Moment::from_tick(20)),
            Ok(ClassicGroupReconciliationTurn::Progress)
        );
    }
    (registry, group_id, schedule)
}

fn drain_reconciliation_loss(registry: &mut GroupConsumerRegistry) {
    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(30)),
        Ok(ClassicGroupReconciliationLossTurn::Progress),
        "active partial-revocation owner becomes lost"
    );
    assert_eq!(
        registry.turn_graceful_revocation(Moment::from_tick(31)),
        Ok(ClassicGroupRevocationTurn::Progress),
        "lost partial-revocation owner settles before reconciliation removal"
    );
    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(32)),
        Ok(ClassicGroupReconciliationLossTurn::Progress),
        "embedded replacement-position owner closes"
    );
    assert_eq!(
        registry.turn_one_classic_group_reconciliation_loss(Moment::from_tick(33)),
        Ok(ClassicGroupReconciliationLossTurn::Progress),
        "exact previous assignment owners retire"
    );
}

fn assert_reconciliation_loss_retired(registry: &mut GroupConsumerRegistry, group_id: GroupId) {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("retired cooperative entry"));
    assert!(entry.classic_reconciliation.is_none());
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.processing_lease.pending_expiration().is_none());
    assert_eq!(entry.fetch.machine_assignment_epoch(), None);
    assert!(entry.revocation.is_dormant());
    assert!(entry.position.is_dormant());
}

fn schedule(
    registry: &super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
) -> kafka_client_core::ClassicHeartbeatSchedule {
    match registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("entry expected"))
        .heartbeat
        .state()
    {
        ClassicHeartbeatExecutionState::Waiting(schedule) => *schedule,
        _ => panic!("waiting Heartbeat expected"),
    }
}
