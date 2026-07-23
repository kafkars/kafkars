//! Bounded terminal recovery after an unexpected host exit.

use kafka_client_core::Moment;

use super::{EngineHostError, EngineHostExit, EngineHostResources, runner::shutdown_driver};

pub(crate) fn recover(
    resources: &mut EngineHostResources,
    primary: EngineHostError,
) -> EngineHostExit {
    let mut failure = primary;
    let mut producer = resources.producer.terminal_host();
    if let Some(cleanup) = producer
        .execution_unavailable(Moment::from_tick(0))
        .err()
        .map(EngineHostError::ProducerStop)
    {
        failure = failure.with_cleanup(cleanup);
    }
    let recovery = producer.recover_notifier();
    drop(producer);
    if let Some(error) = recovery.error {
        failure = failure.with_cleanup(EngineHostError::Completion(error));
    }
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    EngineHostExit {
        notifier: recovery.notifier,
        failure: Some(failure),
    }
}
