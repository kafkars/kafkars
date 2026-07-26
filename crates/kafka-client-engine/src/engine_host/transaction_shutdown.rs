//! Normal and recovery notifier handoff for transaction initialization.

use crate::{completion::NotifierJoin, transaction::TransactionInitializationShardOwner};

use super::{EngineHostError, cleanup::combine_cleanup};

pub(super) fn verify(shard: &TransactionInitializationShardOwner) -> Result<(), EngineHostError> {
    let unsettled = shard.terminal_host().unsettled();
    if unsettled == 0 {
        Ok(())
    } else {
        Err(EngineHostError::TransactionInitialization(
            crate::transaction::TransactionInitializationHostError::Unsettled(unsettled),
        ))
    }
}

pub(super) fn stop(
    shard: &TransactionInitializationShardOwner,
) -> (Result<NotifierJoin, EngineHostError>, Option<NotifierJoin>) {
    let mut host = shard.terminal_host();
    let stopped = host
        .finish_shutdown()
        .map_err(EngineHostError::TransactionInitialization);
    let fallback = host.take_notifier();
    drop(host);
    (stopped, fallback)
}

pub(super) fn recover_after_driver_shutdown(
    shard: &TransactionInitializationShardOwner,
) -> (Option<NotifierJoin>, Option<EngineHostError>) {
    let mut host = shard.terminal_host();
    let recovery = host
        .recover_after_driver_shutdown()
        .err()
        .map(EngineHostError::TransactionInitialization);
    let stopped = host
        .finish_shutdown()
        .map_err(EngineHostError::TransactionInitialization);
    let fallback = host.take_notifier();
    drop(host);
    match stopped {
        Ok(notifier) => (Some(notifier), recovery),
        Err(stop_error) => (fallback, combine_cleanup(recovery, Some(stop_error))),
    }
}
