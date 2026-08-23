//! Exact terminal verification and notifier handoff for every concrete owner.

mod admin_groups;
mod admin_partitions;
mod admin_resources;
mod alter_partition_reassignments;
mod list_partition_reassignments;

use crate::completion::NotifierJoin;

use super::{
    EngineHostError, EngineHostResources, group_consumer_shutdown,
    notifier_shutdown::collect_notification_joins, transaction_shutdown, wait,
};

const SHUTDOWN_TURN_ATTEMPTS: usize = 64;

pub(super) fn shutdown_driver(resources: &mut EngineHostResources) -> Result<(), EngineHostError> {
    let driver = resources
        .driver
        .as_mut()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let turns = driver
        .shutdown_with_turn_limit(SHUTDOWN_TURN_ATTEMPTS, wait::HOST_PARK_LIMIT)
        .map_err(EngineHostError::Driver)?;
    for _turn in 0..turns {
        resources.control.record_driver_turn();
    }
    Ok(())
}

pub(super) fn begin_notification_shutdown(
    resources: &mut EngineHostResources,
) -> Result<(Vec<NotifierJoin>, Option<EngineHostError>), EngineHostError> {
    let mut data = resources.producer.terminal_data();
    let producer = data
        .begin_notification_shutdown()
        .map_err(EngineHostError::ProducerCleanup)?;
    drop(data);
    let admin = resources
        .admin_notifier
        .stop()
        .map_err(EngineHostError::AdminCompletion);
    let admin_fallback = resources.admin_notifier.take_join();
    let assigned_consumer = resources
        .assigned_consumer_notifier
        .stop()
        .map_err(EngineHostError::AssignedConsumerCompletion);
    let assigned_consumer_fallback = resources.assigned_consumer_notifier.take_join();
    let (group_consumer, group_consumer_fallback) =
        group_consumer_shutdown::stop(&resources.group_consumers);
    let group_consumer_recv = resources
        .group_consumers
        .stop_recv_notifier()
        .ok_or(EngineHostError::GroupConsumerRecvNotifierUnavailable);
    let (transaction, transaction_fallback) =
        transaction_shutdown::stop(&resources.transaction_initialization);
    let share_consumer = resources
        .share_consumers
        .stop_close_notifier()
        .map_err(EngineHostError::ShareConsumerCompletion);
    let share_consumer_fallback = resources.share_consumers.take_close_notifier();
    let share_consumer_recv = resources
        .share_consumers
        .stop_recv_notifier()
        .ok_or(EngineHostError::ShareConsumerRecvNotifierUnavailable);
    Ok(collect_notification_joins(
        producer,
        [
            (admin, admin_fallback),
            (assigned_consumer, assigned_consumer_fallback),
            (group_consumer, group_consumer_fallback),
            (group_consumer_recv, None),
            (share_consumer, share_consumer_fallback),
            (share_consumer_recv, None),
            (transaction, transaction_fallback),
        ],
    ))
}

/// Verifies every tracked call and operation before notifier stop.
pub(super) fn prepare_notification_stop(
    resources: &EngineHostResources,
) -> Result<(), EngineHostError> {
    verify_tracked_calls(resources)?;
    verify_admin_operations(resources)?;
    verify_assigned_consumer(resources)?;
    verify_share_consumers(resources)?;
    transaction_shutdown::verify(&resources.transaction_initialization)?;
    let mut data = resources.producer.terminal_data();
    let release = data.verify_release_before_completion();
    let failure = release.err().map(EngineHostError::ProducerCleanup);
    data.drain_terminal_mechanisms();
    let final_failure = data
        .verify_terminal_cleanup()
        .err()
        .map(EngineHostError::ProducerCleanup);
    combine_cleanup(failure, final_failure).map_or(Ok(()), Err)
}

fn verify_share_consumers(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let registry = resources.share_consumers.terminal_registry();
    let unsettled = registry.unsettled();
    if unsettled == 0 {
        Ok(())
    } else {
        Err(EngineHostError::ShareConsumerUnsettled(unsettled))
    }
}

fn verify_assigned_consumer(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let (closed, unsettled, fault) = resources
        .assigned_consumer
        .inspect_terminal(|owner| {
            (
                owner.close_completed(),
                owner.unsettled(),
                owner.fault_kind(),
            )
        })
        .map_err(|error| match error {
            crate::consumer::AssignedConsumerShardLockError::Poisoned => {
                EngineHostError::AssignedConsumerLockPoisoned
            }
            crate::consumer::AssignedConsumerShardLockError::OwnerMissing => {
                EngineHostError::AssignedConsumerOwnerMissing
            }
            crate::consumer::AssignedConsumerShardLockError::Contended => {
                EngineHostError::AssignedConsumerUnsettled(usize::MAX)
            }
        })?;
    if !closed {
        return Err(EngineHostError::AssignedConsumerCloseIncomplete);
    }
    if let Some(fault) = fault {
        return Err(EngineHostError::AssignedConsumerFault(fault));
    }
    if unsettled != 0 {
        return Err(EngineHostError::AssignedConsumerUnsettled(unsettled));
    }
    Ok(())
}

fn verify_tracked_calls(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let produce = resources.produce_calls.retained_count();
    if produce != 0 {
        return Err(EngineHostError::TrackedProduceCallsRemain(produce));
    }
    let identity = resources.producer_identity_calls.retained_count();
    if identity != 0 {
        return Err(EngineHostError::TrackedProducerIdentityCallsRemain(
            identity,
        ));
    }
    let create = resources.create_topics_calls.retained_count();
    if create != 0 {
        return Err(EngineHostError::TrackedCreateTopicsCallsRemain(create));
    }
    let delete = resources.delete_topics_calls.retained_count();
    if delete != 0 {
        return Err(EngineHostError::TrackedDeleteTopicsCallsRemain(delete));
    }
    let describe = resources.describe_cluster_calls.retained_count();
    if describe != 0 {
        return Err(EngineHostError::DescribeClusterCallsRemain(describe));
    }
    let partitions = resources.create_partitions_calls.retained_count();
    if partitions != 0 {
        return Err(EngineHostError::TrackedCreatePartitionsCallsRemain(
            partitions,
        ));
    }
    let topics = resources.describe_topics_calls.retained_count();
    if topics != 0 {
        return Err(EngineHostError::DescribeTopicsCallsRemain(topics));
    }
    let configs = resources.describe_configs_calls.retained_count();
    if configs != 0 {
        return Err(EngineHostError::DescribeConfigsCallsRemain(configs));
    }
    let alter_configs = resources.incremental_alter_configs_calls.retained_count();
    if alter_configs != 0 {
        return Err(EngineHostError::IncrementalAlterConfigsCallsRemain(
            alter_configs,
        ));
    }
    Ok(())
}

fn verify_admin_operations(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    admin_resources::verify(resources)?;
    admin_groups::verify(resources)?;
    admin_partitions::verify(resources)
}

pub(super) fn combine_cleanup(
    primary: Option<EngineHostError>,
    cleanup: Option<EngineHostError>,
) -> Option<EngineHostError> {
    match (primary, cleanup) {
        (Some(primary), Some(cleanup)) => Some(primary.with_cleanup(cleanup)),
        (Some(error), None) | (None, Some(error)) => Some(error),
        (None, None) => None,
    }
}
