//! Assignment-fenced processing acknowledgment and expiration scenarios.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use kafka_client_core::{
    ClassicProcessingLease, ClassicProcessingLeaseEffect, ClassicProcessingLeaseFence,
    ClassicProcessingLeaseInput, ClassicProcessingLeasePolicy, GroupId, GroupPositionFence,
    MemberId, Moment,
};

use super::{
    registry::GroupConsumerRegistry,
    registry_acknowledge::GroupConsumerAcknowledgePortError,
    registry_shard::GroupConsumerShardOwner,
    registry_test_support::{install_session, register, started_registry, stop_registry},
    registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError},
};

#[test]
fn exact_checkpoint_renews_the_existing_processing_owner() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let checkpoint = checkpoint_fence(&registry, group_id);
    let before = processing_fence(&registry, group_id);

    registry
        .acknowledge_processing(group_id, checkpoint, Moment::from_tick(17))
        .unwrap_or_else(|error| panic!("processing acknowledgment: {error:?}"));

    let renewed = registry
        .entry(group_id)
        .and_then(|entry| entry.processing_lease.active_schedule())
        .unwrap_or_else(|| panic!("renewed processing schedule"));
    assert_eq!(renewed.fence(), before);
    assert_eq!(renewed.deadline().tick(), 300_000_000_017);
    stop_registry(&mut registry);
}

#[test]
fn foreign_group_or_member_checkpoint_rejects_without_changing_the_active_schedule() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let checkpoint = checkpoint_fence(&registry, group_id);
    let before = registry
        .entry(group_id)
        .and_then(|entry| entry.processing_lease.active_schedule())
        .unwrap_or_else(|| panic!("active processing schedule"));
    let foreign_group = GroupId::try_from_raw(group_id.get() + 1)
        .unwrap_or_else(|| panic!("foreign group identity"));
    let foreign = GroupPositionFence::new(
        foreign_group,
        checkpoint.membership_cycle(),
        checkpoint.member_id(),
        checkpoint.assignment_generation(),
    );

    assert_eq!(
        registry.acknowledge_processing(group_id, foreign, Moment::from_tick(17)),
        Err(GroupConsumerAcknowledgePortError::StaleCheckpoint)
    );
    let foreign_member = MemberId::try_from_raw(checkpoint.member_id().get() + 1)
        .unwrap_or_else(|| panic!("foreign member identity"));
    let foreign = GroupPositionFence::new(
        group_id,
        checkpoint.membership_cycle(),
        foreign_member,
        checkpoint.assignment_generation(),
    );
    assert_eq!(
        registry.acknowledge_processing(group_id, foreign, Moment::from_tick(19)),
        Err(GroupConsumerAcknowledgePortError::StaleCheckpoint)
    );
    assert_eq!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.processing_lease.active_schedule()),
        Some(before)
    );
    stop_registry(&mut registry);
}

#[test]
fn acknowledgment_at_the_boundary_retains_exact_assignment_loss() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let checkpoint = checkpoint_fence(&registry, group_id);
    replace_processing_lease(&mut registry, group_id, 5, Moment::from_tick(10));

    let error = registry
        .acknowledge_processing(group_id, checkpoint, Moment::from_tick(15))
        .err()
        .unwrap_or_else(|| panic!("due acknowledgment must expire"));
    let GroupConsumerAcknowledgePortError::Expired(expiration) = error else {
        panic!("exact processing expiration expected: {error:?}");
    };
    assert_eq!(
        expiration.schedule().fence(),
        processing_fence(&registry, group_id)
    );
    assert_eq!(expiration.schedule().deadline().tick(), 15);
    assert_eq!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.processing_lease.pending_expiration()),
        Some(expiration)
    );
    stop_registry(&mut registry);
}

#[test]
fn port_expiration_requests_one_host_turn_after_releasing_the_shard() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let checkpoint = checkpoint_fence(&registry, group_id);
    replace_processing_lease(&mut registry, group_id, 1, Moment::from_tick(0));
    let wake = Arc::new(CountingWake::default());
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::clone(&wake),
    );

    assert!(matches!(
        port.try_acknowledge_processing(group_id, checkpoint),
        Err(GroupConsumerAcknowledgePortError::Expired(_))
    ));
    assert_eq!(wake.requests.load(Ordering::Acquire), 1);

    let mut registry = owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[derive(Default)]
struct CountingWake {
    requests: AtomicUsize,
}

impl GroupConsumerShardWake for CountingWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        self.requests.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn checkpoint_fence(registry: &GroupConsumerRegistry, group_id: GroupId) -> GroupPositionFence {
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    GroupPositionFence::new(
        group_id,
        entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active cycle")),
        assignment.member_id(),
        assignment.assignment_generation(),
    )
}

fn processing_fence(
    registry: &GroupConsumerRegistry,
    group_id: GroupId,
) -> ClassicProcessingLeaseFence {
    registry
        .entry(group_id)
        .and_then(|entry| {
            entry
                .processing_lease
                .active_schedule()
                .map(kafka_client_core::ClassicProcessingLeaseSchedule::fence)
                .or_else(|| {
                    entry
                        .processing_lease
                        .pending_expiration()
                        .map(|expiration| expiration.schedule().fence())
                })
        })
        .unwrap_or_else(|| panic!("retained processing fence"))
}

fn replace_processing_lease(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
    timeout_ticks: u64,
    now: Moment,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let checkpoint = {
        let assignment = entry
            .catalog
            .live_assignment()
            .unwrap_or_else(|| panic!("live assignment"));
        ClassicProcessingLeaseFence::new(
            group_id,
            entry
                .classic
                .machine()
                .active_cycle()
                .unwrap_or_else(|| panic!("active cycle")),
            assignment.assignment_generation(),
        )
    };
    entry.processing_lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(timeout_ticks)
            .unwrap_or_else(|error| panic!("processing policy: {error}")),
    );
    let transition = entry
        .processing_lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence: checkpoint,
            now,
        })
        .unwrap_or_else(|error| panic!("processing activation: {error:?}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicProcessingLeaseEffect::Arm { .. })
    ));
}
