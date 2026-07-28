//! Leak-free resource handoff into one self-cleaning native host.

mod list_offsets;

use std::sync::Arc;

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, AdminCompletionPorts, CreatePartitionsHost,
        CreatePartitionsShardOwner, CreateTopicsHost, CreateTopicsShardOwner,
        DeleteConsumerGroupOffsetsHost, DeleteConsumerGroupOffsetsShardOwner, DeleteTopicsHost,
        DeleteTopicsShardOwner, DescribeClusterHost, DescribeClusterShardOwner, DescribeTopicsHost,
        DescribeTopicsShardOwner, IncrementalAlterConfigsHost, IncrementalAlterConfigsShardOwner,
        ListConsumerGroupOffsetsHost, ListConsumerGroupOffsetsShardOwner,
    },
    clock::MonotonicClock,
    config::ValidatedEngineConfig,
    consumer::{GroupConsumerRegistry, GroupConsumerShardOwner},
    driver::DriverOwner,
    producer::ingress::ProducerShardOwner,
};

use super::{
    EngineHostControl, EngineHostResources, EngineLifecycle, EngineStartError,
    alter_consumer_group_offsets_start, assigned_consumer_start, describe_configs_start,
    finalize::finish_host,
    notifier_start,
    start_handoff::{StartedEngineHost, cancel_start, join_cancelled},
    thread_start, transaction_start,
};

#[allow(clippy::too_many_lines)]
pub(crate) fn start(
    config: &EngineConfig,
    validated: ValidatedEngineConfig,
) -> Result<StartedEngineHost, EngineStartError> {
    let lifecycle = Arc::new(EngineLifecycle::new());
    let (sender, handle) = thread_start::start(&lifecycle)?;

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
            config.assigned_consumer_read_isolation().core(),
            Arc::clone(&clock),
            Arc::clone(&wake),
            assigned_publishers.close,
            assigned_publishers.recv,
            assigned_publishers.event,
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
        list_consumer_group_offsets,
        delete_consumer_group_offsets,
        alter_consumer_group_offsets,
        admin_list_offsets,
    } = admin_ports;
    let create_topics = CreateTopicsHost::new(create_topics);
    let delete_topics = DeleteTopicsHost::new(delete_topics);
    let describe_cluster = DescribeClusterHost::new(describe_cluster);
    let create_partitions = CreatePartitionsHost::new(create_partitions);
    let describe_topics = DescribeTopicsHost::new(describe_topics);
    let incremental_alter_configs = IncrementalAlterConfigsHost::new(incremental_alter_configs);
    let list_consumer_group_offsets =
        ListConsumerGroupOffsetsHost::new(list_consumer_group_offsets);
    let delete_consumer_group_offsets =
        DeleteConsumerGroupOffsetsHost::new(delete_consumer_group_offsets);
    let mut group_consumers = match GroupConsumerRegistry::start() {
        Ok(registry) => registry,
        Err(error) => {
            notifier_start::join_acquired(admin_notifier.take_join());
            notifier_start::join_acquired(assigned_consumer_notifier.take_join());
            return cancel_start(sender, handle, EngineStartError::group_consumer(&error));
        }
    };
    let (transaction_initialization, transaction_initialization_admission, producer) =
        match transaction_start::start(
            validated.host_limits,
            Arc::clone(&clock),
            &wake,
            &mut group_consumers,
            &mut admin_notifier,
            &mut assigned_consumer_notifier,
        ) {
            Ok(resources) => resources,
            Err(error) => return cancel_start(sender, handle, error),
        };
    notifier_start::install_thread_ids(
        &lifecycle,
        &producer,
        &admin_notifier,
        &assigned_consumer_notifier,
        &group_consumers,
        &transaction_initialization,
    );
    let (group_consumers, group_consumer) =
        GroupConsumerShardOwner::new(group_consumers, Arc::clone(&clock), Arc::clone(&wake));
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
    let list_consumer_group_offsets = ListConsumerGroupOffsetsShardOwner::new(
        list_consumer_group_offsets,
        Arc::new(driver.reactor_wake()),
    );
    let list_consumer_group_offsets_admission = list_consumer_group_offsets.admission_port();
    let delete_consumer_group_offsets = DeleteConsumerGroupOffsetsShardOwner::new(
        delete_consumer_group_offsets,
        Arc::new(driver.reactor_wake()),
    );
    let delete_consumer_group_offsets_admission = delete_consumer_group_offsets.admission_port();
    let alter_consumer_group_offsets = alter_consumer_group_offsets_start::start(
        alter_consumer_group_offsets,
        driver.reactor_wake(),
    );
    let list_offsets = list_offsets::start(admin_list_offsets, driver.reactor_wake());
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
        list_consumer_group_offsets,
        delete_consumer_group_offsets,
        alter_consumer_group_offsets: alter_consumer_group_offsets.owner,
        list_offsets: list_offsets.owner,
        assigned_consumer: assigned_consumer_owner,
        group_consumers,
        transaction_initialization,
        clock: Arc::clone(&clock),
        control: Arc::clone(&control),
        budget: validated.turn_budget,
        produce_calls,
        producer_identity_calls: crate::driver::TrackedProducerIdentityCalls::new(),
        producer_partitioning_call: None,
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
        list_consumer_group_offsets_admission,
        delete_consumer_group_offsets_admission,
        alter_consumer_group_offsets_admission: alter_consumer_group_offsets.admission,
        list_offsets_admission: list_offsets.admission,
        assigned_consumer,
        group_consumer,
        transaction_initialization: transaction_initialization_admission,
        clock,
        control,
        lifecycle,
    })
}
