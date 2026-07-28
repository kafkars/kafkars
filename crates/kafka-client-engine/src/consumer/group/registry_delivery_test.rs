//! Private immediate group-delivery observation and shard-fencing scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ClassicProcessingLease, ClassicProcessingLeaseEffect, ClassicProcessingLeaseExpirationReason,
    ClassicProcessingLeaseFence, ClassicProcessingLeaseInput, ClassicProcessingLeasePolicy,
    GroupId, GroupPositionBatch, GroupPositionFence, GroupPositionPartitionFact, Moment,
    NextFetchOffset,
};

use super::{
    classic_group_fetch::install_ready_delivery_for_test,
    classic_group_position::test_support::completed_ready,
    registry::GroupConsumerRegistry,
    registry_delivery::{GroupConsumerDeliveryError, GroupConsumerDeliveryPortError},
    registry_processing::GroupConsumerProcessingTurn,
    registry_shard::GroupConsumerShardOwner,
    registry_test_support::{install_session, register, started_registry, stop_registry},
    registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError},
};

struct NoopWake;

impl GroupConsumerShardWake for NoopWake {
    fn request_group_turn(&self) -> Result<(), GroupConsumerShardWakeError> {
        Ok(())
    }
}

#[test]
fn registry_probe_is_group_selected_and_does_not_start_fetch() {
    let mut registry = started_registry();
    let clock = crate::clock::MonotonicClock::new();
    let group_id = register(&mut registry, "workers");
    let unknown = GroupId::try_from_raw(999)
        .unwrap_or_else(|| panic!("unknown group identity must be valid"));

    assert!(matches!(registry.take_delivery(group_id, &clock), Ok(None)));
    assert!(matches!(
        registry.take_delivery(unknown, &clock),
        Err(GroupConsumerDeliveryError::UnknownGroup)
    ));
    assert_eq!(registry.fetch_unsettled(), 0);

    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("close registered group: {error:?}"));
    assert!(matches!(
        registry.take_delivery(group_id, &clock),
        Err(GroupConsumerDeliveryError::Closing)
    ));
    stop_registry(&mut registry);
}

#[test]
fn port_observation_rejects_contention_and_closed_admission_without_waiting() {
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let group_id = register(&mut registry, "workers");
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );

    let registry = owner.lock_registry_for_test();
    assert!(matches!(
        port.try_take_delivery(group_id),
        Err(GroupConsumerDeliveryPortError::Lock(
            super::registry_shard::GroupConsumerShardLockError::Contended
        ))
    ));
    drop(registry);

    port.close_admission();
    assert!(matches!(
        port.try_take_delivery(group_id),
        Err(GroupConsumerDeliveryPortError::Closed)
    ));

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

#[test]
fn port_transfers_and_reclaims_the_exact_fenced_byte_lease_after_close() {
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    install_ready_group_delivery(&mut registry, group_id, 17);
    assert_eq!(registry.fetch_unsettled(), 4);
    let (owner, port) = GroupConsumerShardOwner::new(
        registry,
        Arc::new(crate::clock::MonotonicClock::new()),
        Arc::new(NoopWake),
    );

    let delivery = port
        .try_take_delivery(group_id)
        .unwrap_or_else(|error| panic!("delivery observation: {error:?}"))
        .unwrap_or_else(|| panic!("ready group delivery"));
    let renewed = owner
        .lock_registry_for_test()
        .entry(group_id)
        .and_then(|entry| entry.processing_lease.active_schedule())
        .unwrap_or_else(|| panic!("delivery progress must renew the processing lease"));
    assert_eq!(renewed.fence().group_id(), group_id);
    let position_fence = delivery.position_fence();
    assert_eq!(delivery.group_id(), group_id);
    assert_eq!(delivery.topic(), "orders");
    assert_eq!(delivery.partition(), 0);
    assert_eq!(delivery.next_offset().get(), 20);
    assert_eq!(
        delivery
            .data_batches()
            .iter()
            .flat_map(|batch| &batch.records)
            .count(),
        3
    );

    let registry = owner.lock_registry_for_test();
    let failure = port
        .reclaim_delivery(delivery)
        .err()
        .unwrap_or_else(|| panic!("contended reclaim must reject before transfer"));
    assert_eq!(
        failure.reason(),
        Some(
            super::registry_delivery::GroupConsumerDeliveryReclaimRejection::Lock(
                super::registry_shard::GroupConsumerShardLockError::Contended
            )
        )
    );
    let delivery = failure
        .into_delivery()
        .unwrap_or_else(|| panic!("pre-transfer rejection must return the exact delivery"));
    assert_eq!(delivery.position_fence(), position_fence);
    drop(registry);

    port.close_admission();
    let reclaimed = port.reclaim_delivery(delivery).unwrap_or_else(|failure| {
        panic!("exact reclaim rejected after close: {:?}", failure.reason())
    });
    assert!(!reclaimed.wake_failed());
    let registry = owner
        .try_registry()
        .unwrap_or_else(|error| panic!("registry lock: {error:?}"));
    assert_eq!(registry.fetch_unsettled(), 3);
    assert!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.fetch.activation())
            .is_some()
    );
    drop(registry);

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

#[test]
fn boundary_delivery_is_reclaimed_and_queues_exact_processing_loss() {
    let clock = crate::clock::MonotonicClock::new();
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let fence = replace_processing_lease(&mut registry, group_id, 1);
    install_ready_group_delivery(&mut registry, group_id, 17);

    let error = registry
        .take_delivery(group_id, &clock)
        .err()
        .unwrap_or_else(|| panic!("boundary progress must reject delivery"));
    let GroupConsumerDeliveryError::ProcessingExpired {
        expiration,
        delivery_retained,
    } = error
    else {
        panic!("processing expiration expected: {error:?}");
    };
    assert_eq!(expiration.schedule().fence(), fence);
    assert_eq!(
        expiration.reason(),
        ClassicProcessingLeaseExpirationReason::DeadlineElapsed
    );
    assert!(!delivery_retained);
    assert_eq!(registry.fetch_unsettled(), 3);
    assert_eq!(
        registry
            .entry(group_id)
            .and_then(|entry| entry.processing_lease.pending_expiration()),
        Some(expiration)
    );
    assert_eq!(
        registry
            .turn_processing(
                clock
                    .now()
                    .unwrap_or_else(|error| panic!("clock observation: {error}"))
            )
            .unwrap_or_else(|error| panic!("queued processing loss: {error:?}")),
        GroupConsumerProcessingTurn::Progress
    );
    stop_registry_after_fetch_retirement(&mut registry);
}

#[test]
fn overflowing_delivery_progress_reclaims_bytes_and_queues_assignment_loss() {
    let clock = crate::clock::MonotonicClock::new();
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    install_session(&mut registry, group_id);
    let fence = replace_processing_lease(&mut registry, group_id, u64::MAX);
    install_ready_group_delivery(&mut registry, group_id, 17);

    let error = registry
        .take_delivery(group_id, &clock)
        .err()
        .unwrap_or_else(|| panic!("overflowing progress must reject delivery"));
    let GroupConsumerDeliveryError::ProcessingExpired {
        expiration,
        delivery_retained,
    } = error
    else {
        panic!("processing expiration expected: {error:?}");
    };
    assert_eq!(expiration.schedule().fence(), fence);
    assert_eq!(
        expiration.reason(),
        ClassicProcessingLeaseExpirationReason::DeadlineOverflow
    );
    assert!(!delivery_retained);
    assert_eq!(registry.fetch_unsettled(), 3);
    assert_eq!(
        registry
            .turn_processing(
                clock
                    .now()
                    .unwrap_or_else(|error| panic!("clock observation: {error}"))
            )
            .unwrap_or_else(|error| panic!("queued processing loss: {error:?}")),
        GroupConsumerProcessingTurn::Progress
    );
    stop_registry_after_fetch_retirement(&mut registry);
}

fn install_ready_group_delivery(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
    first_offset: i64,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    let partition = assignment
        .partitions()
        .first()
        .copied()
        .unwrap_or_else(|| panic!("assigned partition"));
    let fence = GroupPositionFence::new(
        assignment.group_id(),
        entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active membership cycle")),
        assignment.member_id(),
        assignment.assignment_generation(),
    );
    entry
        .fetch
        .try_activate(
            completed_ready(
                fence,
                Moment::from_tick(41),
                GroupPositionBatch::new(
                    0,
                    vec![GroupPositionPartitionFact::committed(
                        partition,
                        NextFetchOffset::try_from_raw(first_offset)
                            .unwrap_or_else(|| panic!("next Fetch offset")),
                    )],
                ),
            ),
            fence,
        )
        .unwrap_or_else(|_error| panic!("Fetch activation failed"));
    install_ready_delivery_for_test(&mut entry.fetch, &entry.catalog, first_offset);
}

fn replace_processing_lease(
    registry: &mut GroupConsumerRegistry,
    group_id: GroupId,
    timeout_ticks: u64,
) -> ClassicProcessingLeaseFence {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("registered group"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment"));
    let fence = ClassicProcessingLeaseFence::new(
        group_id,
        entry
            .classic
            .machine()
            .active_cycle()
            .unwrap_or_else(|| panic!("active membership cycle")),
        assignment.assignment_generation(),
    );
    entry.processing_lease = ClassicProcessingLease::new(
        ClassicProcessingLeasePolicy::try_new(timeout_ticks)
            .unwrap_or_else(|error| panic!("processing policy: {error}")),
    );
    let transition = entry
        .processing_lease
        .apply(ClassicProcessingLeaseInput::Activate {
            fence,
            now: Moment::from_tick(0),
        })
        .unwrap_or_else(|error| panic!("processing activation: {error:?}"));
    assert!(matches!(
        transition.effects().next(),
        Some(ClassicProcessingLeaseEffect::Arm { .. })
    ));
    fence
}

fn stop_registry_after_fetch_retirement(registry: &mut GroupConsumerRegistry) {
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
