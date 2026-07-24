//! Exact terminal verification and notifier handoff for every concrete owner.

use crate::completion::NotifierJoin;

use super::{EngineHostError, EngineHostResources, notifier_shutdown::collect_notification_joins};

pub(super) fn begin_notification_shutdown(
    resources: &EngineHostResources,
) -> Result<(Vec<NotifierJoin>, Option<EngineHostError>), EngineHostError> {
    let mut data = resources.producer.terminal_data();
    let producer = data
        .begin_notification_shutdown()
        .map_err(EngineHostError::ProducerCleanup)?;
    drop(data);
    let mut admin_host = resources.admin.terminal_host();
    let admin = admin_host.stop_notifier().map_err(EngineHostError::Admin);
    let fallback = admin_host.recover_notifier();
    drop(admin_host);
    Ok(collect_notification_joins(producer, admin, fallback))
}

/// Verifies every tracked call and operation before notifier stop.
pub(super) fn prepare_notification_stop(
    resources: &EngineHostResources,
) -> Result<(), EngineHostError> {
    verify_tracked_calls(resources)?;
    verify_admin_operations(resources)?;
    let mut data = resources.producer.terminal_data();
    let release = data.verify_release_before_completion();
    let failure = release.err().map(EngineHostError::ProducerCleanup);
    data.drain_terminal_mechanisms();
    let final_failure = data
        .verify_terminal_cleanup()
        .err()
        .map(EngineHostError::ProducerCleanup);
    combine_cleanup(failure, final_failure).map_or(Ok(()), Err)
}

fn verify_tracked_calls(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let produce = resources.produce_calls.retained_count();
    if produce != 0 {
        return Err(EngineHostError::TrackedProduceCallsRemain(produce));
    }
    let create = resources.create_topics_calls.retained_count();
    if create != 0 {
        return Err(EngineHostError::TrackedCreateTopicsCallsRemain(create));
    }
    Ok(())
}

fn verify_admin_operations(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let create = resources.admin.terminal_host().unsettled();
    if create != 0 {
        return Err(EngineHostError::Admin(
            crate::admin::CreateTopicsHostError::Unsettled(create),
        ));
    }
    Ok(())
}

pub(super) fn combine_cleanup(
    primary: Option<EngineHostError>,
    cleanup: Option<EngineHostError>,
) -> Option<EngineHostError> {
    match (primary, cleanup) {
        (Some(primary), Some(cleanup)) => Some(primary.with_cleanup(cleanup)),
        (Some(error), None) | (None, Some(error)) => Some(error),
        (None, None) => None,
    }
}
