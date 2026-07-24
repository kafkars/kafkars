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
    drop(resources.producer.terminal_data());
    drop(resources.create_topics.terminal_host());
    drop(resources.delete_topics.terminal_host());
    drop(resources.describe_cluster.terminal_host());
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    // Even a failed bounded shutdown cannot retain driver-owned bytes past
    // fallback publication: dropping the unique owner tears down the reactor.
    resources.discard_driver_after_shutdown();
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
    #[cfg(test)]
    resources.control.record_recovery_driver_released();

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
    if let Some(notifier) = resources.admin_notifier.take_join() {
        notifiers.push(notifier);
    }
    EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifiers),
        failure: Some(failure),
    }
}
