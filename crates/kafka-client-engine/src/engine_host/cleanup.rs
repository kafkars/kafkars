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
    let mut create_host = resources.create_topics.terminal_host();
    let create = create_host
        .stop_notifier()
        .map_err(EngineHostError::CreateTopics);
    let create_fallback = create_host.recover_notifier();
    drop(create_host);
    let mut delete_host = resources.delete_topics.terminal_host();
    let delete = delete_host
        .stop_notifier()
        .map_err(EngineHostError::DeleteTopics);
    let delete_fallback = delete_host.recover_notifier();
    drop(delete_host);
    let mut describe_host = resources.describe_cluster.terminal_host();
    let describe = describe_host
        .stop_notifier()
        .map_err(EngineHostError::DescribeCluster);
    let describe_fallback = describe_host.recover_notifier();
    drop(describe_host);
    Ok(collect_notification_joins(
        producer,
        [
            (create, create_fallback),
            (delete, delete_fallback),
            (describe, describe_fallback),
        ],
    ))
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
    let delete = resources.delete_topics_calls.retained_count();
    if delete != 0 {
        return Err(EngineHostError::TrackedDeleteTopicsCallsRemain(delete));
    }
    let describe = resources.describe_cluster_calls.retained_count();
    if describe != 0 {
        return Err(EngineHostError::DescribeClusterCallsRemain(describe));
    }
    Ok(())
}

fn verify_admin_operations(resources: &EngineHostResources) -> Result<(), EngineHostError> {
    let create = resources.create_topics.terminal_host().unsettled();
    if create != 0 {
        return Err(EngineHostError::CreateTopics(
            crate::admin::CreateTopicsHostError::Unsettled(create),
        ));
    }
    let delete = resources.delete_topics.terminal_host().unsettled();
    if delete != 0 {
        return Err(EngineHostError::DeleteTopics(
            crate::admin::DeleteTopicsHostError::Unsettled(delete),
        ));
    }
    let describe = resources.describe_cluster.terminal_host().unsettled();
    if describe != 0 {
        return Err(EngineHostError::DescribeCluster(
            crate::admin::DescribeClusterHostError::Unsettled(describe),
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
