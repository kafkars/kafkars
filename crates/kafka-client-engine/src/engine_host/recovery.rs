//! Bounded terminal recovery after an unexpected host exit.

use kafka_client_core::Moment;

use super::{
    EngineHostError, EngineHostExit, EngineHostResources,
    runner::{shutdown_driver, stop_notifier},
};

pub(crate) fn recover(
    resources: &mut EngineHostResources,
    primary: EngineHostError,
) -> Result<EngineHostExit, EngineHostError> {
    let mut failure = primary;
    if let Some(cleanup) = {
        let mut producer = resources.producer.terminal_host();
        producer
            .execution_unavailable(Moment::from_tick(0))
            .err()
            .map(EngineHostError::ProducerStop)
    } {
        failure = failure.with_cleanup(cleanup);
    }
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    let notifier = match stop_notifier(resources) {
        Ok(notifier) => notifier,
        Err(cleanup) => return Err(failure.with_cleanup(cleanup)),
    };
    Ok(EngineHostExit {
        notifier,
        failure: Some(failure),
    })
}
