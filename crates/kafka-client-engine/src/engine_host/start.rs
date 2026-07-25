//! Leak-free resource handoff into one self-cleaning native host.

use std::{
    sync::{Arc, mpsc::sync_channel},
    thread,
};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, AdminCompletionPorts, CreatePartitionsHost,
        CreatePartitionsShardOwner, CreateTopicsHost, CreateTopicsShardOwner, DeleteTopicsHost,
        DeleteTopicsShardOwner, DescribeClusterHost, DescribeClusterShardOwner, DescribeTopicsHost,
        DescribeTopicsShardOwner, IncrementalAlterConfigsHost, IncrementalAlterConfigsShardOwner,
    },
    clock::MonotonicClock,
    config::ValidatedEngineConfig,
    consumer::GroupConsumerRegistry,
    driver::DriverOwner,
    producer::{ProducerHost, ingress::ProducerShardOwner},
};

use super::{
    EngineHostControl, EngineHostResources, EngineLifecycle, EngineStartError,
    assigned_consumer_start, describe_configs_start,
    finalize::finish_host,
    notifier_start,
    start_handoff::{StartedEngineHost, cancel_start, join_cancelled},
};

const HOST_THREAD_NAME: &str = "kafka-client-engine";

// Construction stays rollback-ordered so every resource has a visible reclamation owner.
#[allow(clippy::too_many_lines)]
pub(crate) fn start(
    config: &EngineConfig,
    validated: ValidatedEngineConfig,
) -> Result<StartedEngineHost, EngineStartError> {
    let lifecycle = Arc::new(EngineLifecycle::new());
    let host_lifecycle = Arc::clone(&lifecycle);
    let (sender, receiver) = sync_channel::<EngineHostResources>(1);
    let handle = thread::Builder::new()
        .name(HOST_THREAD_NAME.to_owned())
        .spawn(move || match receiver.recv() {
            Ok(resources) => finish_host(resources, &host_lifecycle),
            Err(_) => host_lifecycle.publish(None),
        })
        .map_err(|error| EngineStartError::host_thread(&error))?;

    let driver = match DriverOwner::build(config) {
        Ok(driver) => driver,
        Err(error) => return cancel_start(sender, handle, EngineStartError::driver(&error)),
    };
    let clock = Arc::new(MonotonicClock::new());
    let wake = Arc::new(driver.reactor_wake());
    let control = Arc::new(EngineHostControl::new(wake.as_ref().clone()));
    let (mut assigned_consumer_notifier, assigned_publishers) =
        match notifier_start::start_assigned_consumer_notifier() {
            Ok(started) => started,
            Err(error) => return cancel_start(sender, handle, error),
        };
    let (assigned_consumer_owner, assigned_consumer) =
        match assigned_consumer_start::start_assigned_consumer(
            Arc::clone(&clock),
            Arc::clone(&wake),
            assigned_publishers.close,
            assigned_publishers.recv,
        ) {
            Ok(owner) => owner,
            Err(error) => {
                notifier_start::join_acquired(assigned_consumer_notifier.take_join());
                return cancel_start(sender, handle, EngineStartError::assigned_consumer(error));
            }
        };
    let (mut admin_notifier, admin_ports) = match AdminCompletionNotifier::start() {
        Ok(owner) => owner,
        Err(error) => {
            notifier_start::join_acquired(assigned_consumer_notifier.take_join());
            return cancel_start(sender, handle, EngineStartError::admin_notifier(&error));
        }
    };
    let AdminCompletionPorts {
        create_topics,
        delete_topics,
        describe_cluster,
        create_partitions,
        describe_topics,
        describe_configs,
        incremental_alter_configs,
    } = admin_ports;
    let create_topics = CreateTopicsHost::new(create_topics);
    let delete_topics = DeleteTopicsHost::new(delete_topics);
    let describe_cluster = DescribeClusterHost::new(describe_cluster);
    let create_partitions = CreatePartitionsHost::new(create_partitions);
    let describe_topics = DescribeTopicsHost::new(describe_topics);
    let incremental_alter_configs = IncrementalAlterConfigsHost::new(incremental_alter_configs);
    let mut group_consumers = match GroupConsumerRegistry::start() {
        Ok(registry) => registry,
        Err(error) => {
            notifier_start::join_acquired(admin_notifier.take_join());
            notifier_start::join_acquired(assigned_consumer_notifier.take_join());
            return cancel_start(sender, handle, EngineStartError::group_consumer(&error));
        }
    };
    let producer = match ProducerHost::new_with_compression_wake(validated.host_limits, &wake) {
        Ok(producer) => producer,
        Err(error) => {
            notifier_start::join_acquired(group_consumers.take_notifier());
            notifier_start::join_acquired(admin_notifier.take_join());
            notifier_start::join_acquired(assigned_consumer_notifier.take_join());
            return cancel_start(sender, handle, EngineStartError::producer(&error));
        }
    };
    notifier_start::install_thread_ids(
        &lifecycle,
        &producer,
        &admin_notifier,
        &assigned_consumer_notifier,
        &group_consumers,
    );
    let producer = ProducerShardOwner::new(producer, Arc::clone(&wake));
    let admission = producer.admission_port();
    let create_topics = CreateTopicsShardOwner::new(create_topics, Arc::new(driver.reactor_wake()));
    let create_topics_admission = create_topics.admission_port();
    let delete_topics = DeleteTopicsShardOwner::new(delete_topics, Arc::new(driver.reactor_wake()));
    let delete_topics_admission = delete_topics.admission_port();
    let describe_cluster =
        DescribeClusterShardOwner::new(describe_cluster, Arc::new(driver.reactor_wake()));
    let describe_cluster_admission = describe_cluster.admission_port();
    let create_partitions =
        CreatePartitionsShardOwner::new(create_partitions, Arc::new(driver.reactor_wake()));
    let create_partitions_admission = create_partitions.admission_port();
    let describe_topics =
        DescribeTopicsShardOwner::new(describe_topics, Arc::new(driver.reactor_wake()));
    let describe_topics_admission = describe_topics.admission_port();
    let describe_configs =
        describe_configs_start::start(describe_configs, Arc::new(driver.reactor_wake()));
    let incremental_alter_configs = IncrementalAlterConfigsShardOwner::new(
        incremental_alter_configs,
        Arc::new(driver.reactor_wake()),
    );
    let incremental_alter_configs_admission = incremental_alter_configs.admission_port();
    let produce_calls =
        crate::driver::TrackedProduceCalls::new(validated.host_limits.batch_capacity);
    let resources = EngineHostResources {
        driver: Some(driver),
        producer,
        admin_notifier,
        assigned_consumer_notifier,
        create_topics,
        delete_topics,
        describe_cluster,
        create_partitions,
        describe_topics,
        describe_configs: describe_configs.owner,
        incremental_alter_configs,
        assigned_consumer: assigned_consumer_owner,
        group_consumers,
        clock: Arc::clone(&clock),
        control: Arc::clone(&control),
        budget: validated.turn_budget,
        produce_calls,
        producer_identity_calls: crate::driver::TrackedProducerIdentityCalls::new(),
        create_topics_calls: crate::driver::TrackedCreateTopicsCalls::new(
            crate::admin::CREATE_TOPICS_CAPACITY,
        ),
        delete_topics_calls: crate::driver::TrackedDeleteTopicsCalls::new(
            crate::admin::DELETE_TOPICS_CAPACITY,
        ),
        describe_cluster_calls: crate::driver::DescribeClusterCalls::new(
            crate::admin::DESCRIBE_CLUSTER_CAPACITY,
        ),
        create_partitions_calls: crate::driver::TrackedCreatePartitionsCalls::new(
            crate::admin::CREATE_PARTITIONS_CAPACITY,
        ),
        describe_topics_calls: crate::driver::DescribeTopicsCalls::new(
            crate::admin::DESCRIBE_TOPICS_CAPACITY,
        ),
        describe_configs_calls: describe_configs.calls,
        incremental_alter_configs_calls: crate::driver::IncrementalAlterConfigsCalls::new(
            crate::admin::INCREMENTAL_ALTER_CONFIGS_CAPACITY,
        ),
    };
    if let Err(error) = sender.send(resources) {
        control.request_shutdown();
        finish_host(error.0, &lifecycle);
        join_cancelled(handle);
        return Err(EngineStartError::handoff());
    }

    // The host self-cleans and publishes a retained terminal report after
    // joining its notifier. External shutdown observes that report, not this
    // operating-system join token.
    drop(handle);
    Ok(StartedEngineHost {
        admission,
        create_topics_admission,
        delete_topics_admission,
        describe_cluster_admission,
        create_partitions_admission,
        describe_topics_admission,
        describe_configs_admission: describe_configs.admission,
        incremental_alter_configs_admission,
        assigned_consumer,
        clock,
        control,
        lifecycle,
    })
}
