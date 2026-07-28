//! Hosted classic processing-liveness scheduling and exact loss scenarios.

use kafka_client_core::{
    ClassicGroupPhase, ClassicProcessingLease, ClassicProcessingLeaseEffect,
    ClassicProcessingLeaseFence, ClassicProcessingLeaseInput, ClassicProcessingLeasePolicy,
    GroupId, GroupOffsetCommitAdmissionErrorKind, Moment,
};

use super::{
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_test_support,
    offset_commit::GroupOffsetCommitAdmissionFailureKind,
    registry::GroupConsumerRegistry,
    registry_commit::GroupConsumerCommitFailureKind,
    registry_processing::GroupConsumerProcessingTurn,
    registry_test_support::{
        checkpoint, deadline, install_session, register, started_registry, stop_registry,
    },
};
use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

#[test]
fn registered_processing_policy_arms_independently_of_session_and_heartbeat() {
    let mut registry = started_registry();
    let processing_policy = ClassicProcessingLeasePolicy::try_new(17)
        .unwrap_or_else(|error| panic!("processing policy: {error}"));
    let group_id = registry
        .try_register_with_processing_policy(
            std::sync::Arc::from("workers"),
            vec![std::sync::Arc::from("orders")],
            classic_group_test_support::timing(),
            classic_group_test_support::heartbeat_policy(),
            classic_group_test_support::rejoin_policy(),
            processing_policy,
        )
        .unwrap_or_else(|failure| panic!("registration failed: {:?}", failure.kind));
    install_session(&mut registry, group_id);

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let processing = entry
        .processing_lease
        .active_schedule()
        .unwrap_or_else(|| panic!("processing lease must be armed"));
    assert_eq!(processing.deadline().tick(), 20);
    assert_eq!(
        entry.classic.machine().timing().session_timeout_ms(),
        10_000
    );
    let ClassicHeartbeatExecutionState::Waiting(heartbeat) = entry.heartbeat.state() else {
        panic!("heartbeat schedule must remain independently armed");
    };
    assert_eq!(heartbeat.due().tick(), 3);
    assert_eq!(heartbeat.liveness_deadline().tick(), 10_000_000_002);
    assert_ne!(processing.deadline(), heartbeat.due());
    assert_ne!(processing.deadline(), heartbeat.liveness_deadline());

    stop_registry(&mut registry);
}

#[test]
fn registry_observes_the_exact_processing_deadline_and_counts_its_owner() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let schedule = replace_processing_lease(&mut registry, group_id, 1, Moment::from_tick(0));

    assert_eq!(schedule.deadline().tick(), 1);
    assert_eq!(
        registry.processing_next_deadline(),
        Some(schedule.deadline())
    );
    assert_eq!(registry.next_deadline(), Some(schedule.deadline()));
    assert_eq!(registry.processing_unsettled(), 1);

    assert_eq!(
        registry
            .turn_processing(Moment::from_tick(0))
            .unwrap_or_else(|error| panic!("early processing turn: {error:?}")),
        GroupConsumerProcessingTurn::Idle
    );
    assert_eq!(
        registry.processing_next_deadline(),
        Some(schedule.deadline())
    );
    stop_registry(&mut registry);
}

#[test]
fn host_runs_due_processing_loss_before_heartbeat_and_revokes_the_assignment() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let checkpoint = checkpoint(&registry, group_id);
    let schedule = replace_processing_lease(&mut registry, group_id, 5, Moment::from_tick(10));

    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    assert!(
        registry
            .turn(
                Moment::from_tick(schedule.deadline().tick()),
                &MonotonicClock::new(),
                &driver,
            )
            .unwrap_or_else(|error| panic!("due host turn: {error}"))
            .progressed
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.processing_lease.active_schedule().is_none());
    assert!(entry.processing_lease.pending_expiration().is_none());
    assert!(entry.heartbeat.is_dormant());
    assert_eq!(registry.processing_unsettled(), 0);
    let failure = registry
        .try_commit(group_id, deadline(100), checkpoint)
        .err()
        .unwrap_or_else(|| panic!("lost checkpoint must reject"));
    assert_eq!(
        failure.kind,
        GroupConsumerCommitFailureKind::OffsetCommit(GroupOffsetCommitAdmissionFailureKind::Core(
            GroupOffsetCommitAdmissionErrorKind::AssignmentLost
        ))
    );
    stop_registry(&mut registry);
}

fn replace_processing_lease(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
    timeout_ticks: u64,
    now: Moment,
) -> kafka_client_core::ClassicProcessingLeaseSchedule {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("active membership cycle"));
    let fence =
        ClassicProcessingLeaseFence::new(group_id, cycle, assignment.assignment_generation());
    entry.processing_lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(timeout_ticks)
            .unwrap_or_else(|error| panic!("processing policy: {error}")),
    );
    let transition = entry
        .processing_lease
        .apply(ClassicProcessingLeaseInput::Activate { fence, now })
        .unwrap_or_else(|error| panic!("processing activation: {error:?}"));
    let mut effects = transition.effects();
    match (effects.next(), effects.next()) {
        (Some(ClassicProcessingLeaseEffect::Arm { schedule }), None) => *schedule,
        _ => panic!("one processing schedule expected"),
    }
}
