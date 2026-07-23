//! Bounded terminal recovery after an unexpected host exit.

use kafka_client_core::Moment;

use super::{EngineHostError, EngineHostExit, EngineHostResources, runner::shutdown_driver};

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
    let release = producer.verify_release_before_completion();
    let pending_blocked = release
        .as_ref()
        .is_err_and(|error| error.pending_ownership().is_some());
    if let Some(cleanup) = release.err().map(EngineHostError::ProducerCleanup) {
        failure = failure.with_cleanup(cleanup);
    }
    let notifier = if pending_blocked {
        None
    } else {
        if let Some(cleanup) = producer
            .drain_terminal_mechanisms()
            .err()
            .map(EngineHostError::ProducerCleanup)
        {
            failure = failure.with_cleanup(cleanup);
        }
        if let Some(cleanup) = producer
            .verify_terminal_cleanup()
            .err()
            .map(EngineHostError::ProducerCleanup)
        {
            failure = failure.with_cleanup(cleanup);
        }
        match producer.recover_notifier() {
            Ok(recovery) => {
                if let Some(error) = recovery.error {
                    failure = failure.with_cleanup(EngineHostError::ProducerCleanup(error.into()));
                }
                recovery.notifier
            }
            Err(error) => {
                failure = failure.with_cleanup(EngineHostError::ProducerCleanup(error));
                None
            }
        }
    };
    drop(producer);
    if let Some(cleanup) = shutdown_driver(resources).err() {
        failure = failure.with_cleanup(cleanup);
    }
    EngineHostExit {
        notifier,
        failure: Some(failure),
    }
}
