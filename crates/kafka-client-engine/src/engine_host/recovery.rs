//! Bounded terminal recovery after an unexpected host exit.

use kafka_client_core::Moment;

use super::{
    EngineHostError, EngineHostExit, EngineHostResources,
    runner::{NotifierShutdownOwner, shutdown_driver},
};

pub(crate) fn recover(
    resources: &mut EngineHostResources,
    primary: EngineHostError,
) -> EngineHostExit {
    let mut failure = primary;
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
    let notifier = NotifierShutdownOwner::new(recovery.notifier);
    if let Some(error) = recovery.error {
        failure = failure.with_cleanup(EngineHostError::ProducerCleanup(error.into()));
    }
    drop(producer);
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    EngineHostExit {
        notifier,
        failure: Some(failure),
    }
}
