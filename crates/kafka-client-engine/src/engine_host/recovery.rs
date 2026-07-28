//! Bounded terminal recovery after an unexpected host exit.

use kafka_client_core::Moment;

use super::{
    EngineHostError, EngineHostExit, EngineHostResources, admin, group_consumer_shutdown,
    notifier_shutdown::NotifierShutdownOwner, runner::shutdown_driver, transaction_shutdown,
};

impl EngineHostResources {
    fn discard_driver_after_shutdown(&mut self) {
        drop(self.driver.take());
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "recovery ordering is one auditable linear shutdown sequence"
)]
pub(crate) fn recover(
    resources: &mut EngineHostResources,
    primary: EngineHostError,
) -> EngineHostExit {
    // Fence application admission before any execution owner is quiesced.
    let mut failure = close_consumer_admission(resources, primary);
    resources
        .transaction_initialization
        .admission_port()
        .close_admission();
    drop(resources.producer.terminal_data());
    drop(resources.create_topics.terminal_host());
    drop(resources.delete_topics.terminal_host());
    drop(resources.describe_cluster.terminal_host());
    drop(resources.create_partitions.terminal_host());
    drop(resources.describe_topics.terminal_host());
    drop(resources.describe_configs.terminal_host());
    drop(resources.incremental_alter_configs.terminal_host());
    drop(resources.list_consumer_group_offsets.terminal_host());
    drop(resources.delete_consumer_group_offsets.terminal_host());
    drop(resources.alter_consumer_group_offsets.terminal_host());
    drop(resources.list_offsets.terminal_host());
    drop(resources.list_partition_reassignments.terminal_host());
    drop(resources.alter_partition_reassignments.terminal_host());
    drop(resources.describe_log_dirs.terminal_host());
    drop(resources.alter_replica_log_dirs.terminal_host());
    drop(resources.delete_records.terminal_host());
    drop(resources.transaction_initialization.terminal_host());
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    // Even a failed bounded shutdown cannot retain driver-owned bytes past
    // fallback publication: dropping the unique owner tears down the reactor.
    resources.discard_driver_after_shutdown();
    #[cfg(test)]
    resources.control.record_recovery_driver_released();
    resources.produce_calls.discard_after_driver_shutdown();
    resources
        .producer_identity_calls
        .discard_after_driver_shutdown();
    super::produce::discard_partitioning_after_driver_shutdown(
        &mut resources.producer_partitioning_call,
    );
    resources
        .create_topics_calls
        .discard_after_driver_shutdown();
    resources
        .delete_topics_calls
        .discard_after_driver_shutdown();
    resources
        .describe_cluster_calls
        .discard_after_driver_shutdown();
    resources
        .create_partitions_calls
        .discard_after_driver_shutdown();
    resources
        .describe_topics_calls
        .discard_after_driver_shutdown();
    resources
        .describe_configs_calls
        .discard_after_driver_shutdown();
    resources
        .incremental_alter_configs_calls
        .discard_after_driver_shutdown();
    failure = recover_assigned_after_driver_shutdown(resources, failure);
    let assigned_consumer_notifier = resources.assigned_consumer_notifier.take_join();
    #[cfg(test)]
    resources.control.record_group_recovery_started();
    let (group_consumer_notifier, group_consumer_failure) =
        group_consumer_shutdown::recover_after_driver_shutdown(&resources.group_consumers);
    if let Some(cleanup) = group_consumer_failure {
        failure = failure.with_cleanup(cleanup);
    }
    let group_consumer_recv_notifier = resources.group_consumers.stop_recv_notifier();
    if group_consumer_recv_notifier.is_none() {
        failure = failure.with_cleanup(EngineHostError::GroupConsumerRecvNotifierUnavailable);
    }
    let (transaction_notifier, transaction_failure) =
        transaction_shutdown::recover_after_driver_shutdown(&resources.transaction_initialization);
    if let Some(cleanup) = transaction_failure {
        failure = failure.with_cleanup(cleanup);
    }

    let mut producer = resources.producer.terminal_data();
    if let Some(cleanup) = producer
        .execution_unavailable(Moment::from_tick(0))
        .err()
        .map(EngineHostError::ProducerStop)
    {
        failure = failure.with_cleanup(cleanup);
    }
    if let Some(cleanup) = producer
        .verify_release_before_completion()
        .err()
        .map(EngineHostError::ProducerCleanup)
    {
        failure = failure.with_cleanup(cleanup);
    }
    producer.drain_terminal_mechanisms();
    if let Some(cleanup) = producer
        .verify_terminal_cleanup()
        .err()
        .map(EngineHostError::ProducerCleanup)
    {
        failure = failure.with_cleanup(cleanup);
    }
    let recovery = producer.recover_notifier();
    let mut notifiers = Vec::with_capacity(6);
    if let Some(notifier) = recovery.notifier {
        notifiers.push(notifier);
    }
    if let Some(error) = recovery.error {
        failure = failure.with_cleanup(EngineHostError::ProducerCleanup(error.into()));
    }
    drop(producer);
    failure = admin::recovery::recover_operations(resources, failure);
    if let Some(notifier) = resources.admin_notifier.take_join() {
        notifiers.push(notifier);
    }
    if let Some(notifier) = assigned_consumer_notifier {
        notifiers.push(notifier);
    }
    if let Some(notifier) = group_consumer_notifier {
        notifiers.push(notifier);
    }
    if let Some(notifier) = group_consumer_recv_notifier {
        notifiers.push(notifier);
    }
    if let Some(notifier) = transaction_notifier {
        notifiers.push(notifier);
    }
    EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifiers),
        failure: Some(failure),
    }
}

fn close_consumer_admission(
    resources: &mut EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    if let Err(cleanup) = resources.assigned_consumer.close_assigned_admission() {
        failure = failure.with_cleanup(match cleanup {
            crate::consumer::AssignedConsumerShardLockError::Poisoned => {
                EngineHostError::AssignedConsumerLockPoisoned
            }
            crate::consumer::AssignedConsumerShardLockError::OwnerMissing => {
                EngineHostError::AssignedConsumerOwnerMissing
            }
            crate::consumer::AssignedConsumerShardLockError::Contended => {
                EngineHostError::AssignedConsumerUnsettled(usize::MAX)
            }
        });
    }
    resources.group_consumers.close_admission();
    failure
}

fn recover_assigned_after_driver_shutdown(
    resources: &mut EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    #[cfg(test)]
    resources.control.record_assigned_recovery_started();
    match resources
        .assigned_consumer
        .take_assigned_owner_after_driver_shutdown()
    {
        Ok(report) if report.requires_cleanup_report() => {
            failure =
                failure.with_cleanup(EngineHostError::AssignedConsumerRecovery(Box::new(report)));
        }
        Ok(_report) => {}
        Err(crate::consumer::AssignedConsumerShardLockError::OwnerMissing) => {
            failure = failure.with_cleanup(EngineHostError::AssignedConsumerOwnerMissing);
        }
        Err(crate::consumer::AssignedConsumerShardLockError::Poisoned) => {
            failure = failure.with_cleanup(EngineHostError::AssignedConsumerLockPoisoned);
        }
        Err(crate::consumer::AssignedConsumerShardLockError::Contended) => {
            failure = failure.with_cleanup(EngineHostError::AssignedConsumerUnsettled(usize::MAX));
        }
    }
    failure
}
