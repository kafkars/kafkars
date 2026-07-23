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
    drop(resources.admin.terminal_host());
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    // Even a failed bounded shutdown cannot retain driver-owned bytes past
    // fallback publication: dropping the unique owner tears down the reactor.
    resources.discard_driver_after_shutdown();
    resources.produce_calls.discard_after_driver_shutdown();
    resources
        .create_topics_calls
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
    let mut admin = resources.admin.terminal_host();
    if let Some(cleanup) = admin
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::Admin)
    {
        failure = failure.with_cleanup(cleanup);
    }
    if let Some(notifier) = admin.recover_notifier() {
        notifiers.push(notifier);
    }
    drop(admin);
    EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifiers),
        failure: Some(failure),
    }
}
