//! Share membership release and notifier handoff after unique driver teardown.

use crate::completion::NotifierJoin;

use super::super::{EngineHostError, EngineHostResources};

pub(in crate::engine_host) fn recover(
    resources: &mut EngineHostResources,
    mut failure: EngineHostError,
    notifiers: &mut Vec<NotifierJoin>,
) -> EngineHostError {
    if let Err(error) = resources.share_consumers.recover_after_driver_shutdown() {
        failure = failure.with_cleanup(EngineHostError::ShareConsumer(error));
    }
    if let Some(notifier) = resources.share_consumers.take_close_notifier() {
        notifiers.push(notifier);
    }
    match resources.share_consumers.stop_recv_notifier() {
        Some(notifier) => notifiers.push(notifier),
        None => {
            failure = failure.with_cleanup(EngineHostError::ShareConsumerRecvNotifierUnavailable);
        }
    }
    failure
}
