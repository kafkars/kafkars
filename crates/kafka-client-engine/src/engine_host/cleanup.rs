//! Exact terminal verification and notifier handoff for every concrete owner.

use crate::completion::NotifierJoin;

use super::{EngineHostError, EngineHostResources, notifier_shutdown::collect_notification_joins};

pub(super) fn begin_notification_shutdown(
    resources: &mut EngineHostResources,
) -> Result<(Vec<NotifierJoin>, Option<EngineHostError>), EngineHostError> {
    let mut data = resources.producer.terminal_data();
    let producer = data
        .begin_notification_shutdown()
        .map_err(EngineHostError::ProducerCleanup)?;
    drop(data);
    let admin = resources
        .admin_notifier
        .stop()
        .map_err(EngineHostError::AdminCompletion);
    let admin_fallback = resources.admin_notifier.take_join();
    Ok(collect_notification_joins(
        producer,
        [(admin, admin_fallback)],
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
    let identity = resources.producer_identity_calls.retained_count();
    if identity != 0 {
        return Err(EngineHostError::TrackedProducerIdentityCallsRemain(
            identity,
        ));
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
    let partitions = resources.create_partitions_calls.retained_count();
    if partitions != 0 {
        return Err(EngineHostError::TrackedCreatePartitionsCallsRemain(
            partitions,
        ));
    }
    let topics = resources.describe_topics_calls.retained_count();
    if topics != 0 {
        return Err(EngineHostError::DescribeTopicsCallsRemain(topics));
    }
    let configs = resources.describe_configs_calls.retained_count();
    if configs != 0 {
        return Err(EngineHostError::DescribeConfigsCallsRemain(configs));
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
    let partitions = resources.create_partitions.terminal_host().unsettled();
    if partitions != 0 {
        return Err(EngineHostError::CreatePartitions(
            crate::admin::CreatePartitionsHostError::Unsettled(partitions),
        ));
    }
    let topics = resources.describe_topics.terminal_host().unsettled();
    if topics != 0 {
        return Err(EngineHostError::DescribeTopics(
            crate::admin::DescribeTopicsHostError::Unsettled(topics),
        ));
    }
    let configs = resources.describe_configs.terminal_host().unsettled();
    if configs != 0 {
        return Err(EngineHostError::DescribeConfigs(
            crate::admin::DescribeConfigsHostError::Unsettled(configs),
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
