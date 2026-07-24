//! Bounded terminal recovery after an unexpected host exit.

use kafka_client_core::Moment;

use super::{
    EngineHostError, EngineHostExit, EngineHostResources, notifier_shutdown::NotifierShutdownOwner,
    runner::shutdown_driver,
};

impl EngineHostResources {
    fn discard_driver_after_shutdown(&mut self) {
        drop(self.driver.take());
    }
}

pub(crate) fn recover(
    resources: &mut EngineHostResources,
    primary: EngineHostError,
) -> EngineHostExit {
    let mut failure = primary;
    // Fence application admission before any execution owner is quiesced.
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
    drop(resources.producer.terminal_data());
    drop(resources.create_topics.terminal_host());
    drop(resources.delete_topics.terminal_host());
    drop(resources.describe_cluster.terminal_host());
    drop(resources.create_partitions.terminal_host());
    drop(resources.describe_topics.terminal_host());
    drop(resources.describe_configs.terminal_host());
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
    failure = recover_assigned_after_driver_shutdown(resources, failure);

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
    let mut notifiers = Vec::with_capacity(2);
    if let Some(notifier) = recovery.notifier {
        notifiers.push(notifier);
    }
    if let Some(error) = recovery.error {
        failure = failure.with_cleanup(EngineHostError::ProducerCleanup(error.into()));
    }
    drop(producer);
    failure = recover_admin_operations(resources, failure);
    if let Some(notifier) = resources.admin_notifier.take_join() {
        notifiers.push(notifier);
    }
    EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifiers),
        failure: Some(failure),
    }
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

fn recover_admin_operations(
    resources: &EngineHostResources,
    mut failure: EngineHostError,
) -> EngineHostError {
    let mut create_topics = resources.create_topics.terminal_host();
    if let Some(cleanup) = create_topics
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::CreateTopics)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(create_topics);
    let mut delete_topics = resources.delete_topics.terminal_host();
    if let Some(cleanup) = delete_topics
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DeleteTopics)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(delete_topics);
    let mut describe_cluster = resources.describe_cluster.terminal_host();
    if let Some(cleanup) = describe_cluster
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeCluster)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_cluster);
    let mut create_partitions = resources.create_partitions.terminal_host();
    if let Some(cleanup) = create_partitions
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::CreatePartitions)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(create_partitions);
    let mut describe_topics = resources.describe_topics.terminal_host();
    if let Some(cleanup) = describe_topics
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeTopics)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_topics);
    let mut describe_configs = resources.describe_configs.terminal_host();
    if let Some(cleanup) = describe_configs
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::DescribeConfigs)
    {
        failure = failure.with_cleanup(cleanup);
    }
    drop(describe_configs);
    failure
}
