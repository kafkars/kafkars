//! Normal and post-driver-shutdown notifier handoff for the group registry.

use crate::{completion::NotifierJoin, consumer::GroupConsumerRegistry};

use super::{EngineHostError, cleanup::combine_cleanup};

pub(super) fn stop(
    registry: &mut GroupConsumerRegistry,
) -> (Result<NotifierJoin, EngineHostError>, Option<NotifierJoin>) {
    let stopped = registry
        .finish_shutdown()
        .map_err(EngineHostError::GroupConsumer);
    let fallback = registry.take_notifier();
    (stopped, fallback)
}

pub(super) fn recover_after_driver_shutdown(
    registry: &mut GroupConsumerRegistry,
) -> (Option<NotifierJoin>, Option<EngineHostError>) {
    registry.close_admission();
    let recovery = registry
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::GroupConsumer);
    let (stopped, fallback) = stop(registry);
    match stopped {
        Ok(notifier) => (Some(notifier), recovery),
        Err(stop_error) => (fallback, combine_cleanup(recovery, Some(stop_error))),
    }
}
