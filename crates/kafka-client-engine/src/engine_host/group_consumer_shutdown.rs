//! Normal and post-driver-shutdown notifier handoff for the group registry.

use crate::{completion::NotifierJoin, consumer::GroupConsumerShardOwner};

use super::{EngineHostError, cleanup::combine_cleanup};

pub(super) fn stop(
    shard: &GroupConsumerShardOwner,
) -> (Result<NotifierJoin, EngineHostError>, Option<NotifierJoin>) {
    let mut registry = shard.terminal_registry();
    let stopped = registry
        .finish_shutdown()
        .map_err(EngineHostError::GroupConsumer);
    let fallback = registry.take_notifier();
    drop(registry);
    (stopped, fallback)
}

pub(super) fn recover_after_driver_shutdown(
    shard: &GroupConsumerShardOwner,
) -> (Option<NotifierJoin>, Option<EngineHostError>) {
    let mut registry = shard.terminal_registry();
    registry.close_admission();
    let recovery = registry
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::GroupConsumer);
    let stopped = registry
        .finish_shutdown()
        .map_err(EngineHostError::GroupConsumer);
    let fallback = registry.take_notifier();
    drop(registry);
    match stopped {
        Ok(notifier) => (Some(notifier), recovery),
        Err(stop_error) => (fallback, combine_cleanup(recovery, Some(stop_error))),
    }
}
