//! Opt-in real-broker evidence for safe public partition-reassignment administration.

#[allow(
    dead_code,
    reason = "this isolated target compiles only its subset of shared broker helpers"
)]
#[path = "real_broker_support/mod.rs"]
mod real_broker_support;

use std::io;

use kafka_client::{
    Admin, Client, PartitionReassignmentChange, TopicDescription, TopicPartition,
    TopicPartitionDescription,
};
use real_broker_support::{
    OPERATION_TIMEOUT, TestError, TopicCleanup, client_builder_from_environment, create_topics,
    delete_topics, ready_client, unique_name, wait_within,
};

#[test]
#[ignore = "requires smoke environment variables and a mutable Kafka cluster"]
fn public_partition_reassignment_noop_round_trip_and_cleanup() {
    run().unwrap_or_else(|error| panic!("real Kafka partition-reassignment smoke failed: {error}"));
}

fn run() -> Result<(), TestError> {
    let topic = unique_name("kafka-client-partition-reassignment-smoke");
    let client = client_builder_from_environment("kafka-client-real-partition-reassignment-smoke")?
        .build()?;
    let workflow = run_with_client(&client, &topic);

    wait_within(
        client.shutdown(),
        "partition-reassignment smoke client shutdown",
    )??;
    workflow
}

fn run_with_client(client: &Client, topic: &str) -> Result<(), TestError> {
    let admin = client.admin();
    let topic_owned = topic.to_owned();
    ready_client(client)?;
    create_topics(&admin, std::slice::from_ref(&topic_owned))?;
    let mut cleanup = TopicCleanup::new(admin.clone(), vec![topic_owned.clone()]);

    let initial = describe_topic(&admin, topic)?;
    let initial_partition = require_single_healthy_partition(&initial, topic)?;
    let replicas = initial_partition.replicas().to_vec();
    if replicas.len() != 1 {
        return Err(io::Error::other(format!(
            "single-broker reassignment smoke observed {} replicas",
            replicas.len()
        ))
        .into());
    }

    alter_to_exact_current_assignment(&admin, topic, &replicas)?;
    require_no_active_reassignment(&admin, topic)?;

    let stable = describe_topic(&admin, topic)?;
    require_stable_assignment(&initial, &stable, topic, &replicas)?;

    delete_topics(&admin, std::slice::from_ref(&topic_owned))?;
    cleanup.disarm();
    Ok(())
}

fn describe_topic(admin: &Admin, topic: &str) -> Result<TopicDescription, TestError> {
    let described = wait_within(
        admin
            .describe_topics([topic])
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "DescribeTopics for partition reassignment",
    )??;
    let mut entries = described.into_entries();
    if entries.len() != 1 {
        return Err(io::Error::other(format!(
            "DescribeTopics returned {} entries instead of one",
            entries.len()
        ))
        .into());
    }
    let (returned_topic, outcome) = entries
        .pop()
        .ok_or_else(|| io::Error::other("DescribeTopics omitted its topic outcome"))?;
    if returned_topic != topic {
        return Err(io::Error::other(format!(
            "DescribeTopics returned {returned_topic:?} instead of {topic:?}"
        ))
        .into());
    }
    Ok(outcome?)
}

fn require_single_healthy_partition<'a>(
    description: &'a TopicDescription,
    topic: &str,
) -> Result<&'a TopicPartitionDescription, TestError> {
    if description.name() != topic
        || description.is_internal()
        || description.partitions().len() != 1
    {
        return Err(io::Error::other(
            "DescribeTopics changed the fixture identity or partition count",
        )
        .into());
    }
    let partition = &description.partitions()[0];
    if partition.partition_index() != 0
        || partition.error().is_some()
        || partition.leader_id().is_none()
        || partition.replicas() != partition.in_sync_replicas()
        || !partition.offline_replicas().is_empty()
    {
        return Err(io::Error::other(
            "DescribeTopics did not return one healthy in-sync partition",
        )
        .into());
    }
    Ok(partition)
}

fn alter_to_exact_current_assignment(
    admin: &Admin,
    topic: &str,
    replicas: &[i32],
) -> Result<(), TestError> {
    let altered = wait_within(
        admin
            .alter_partition_reassignments([PartitionReassignmentChange::new(
                topic,
                0,
                replicas.iter().copied(),
            )])
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "AlterPartitionReassignments exact-current assignment",
    )??;
    let mut entries = altered.into_partitions().into_entries();
    if entries.len() != 1 {
        return Err(io::Error::other(format!(
            "AlterPartitionReassignments returned {} entries instead of one",
            entries.len()
        ))
        .into());
    }
    let (target, outcome) = entries.pop().ok_or_else(|| {
        io::Error::other("AlterPartitionReassignments omitted its partition outcome")
    })?;
    require_target(&target, topic, "AlterPartitionReassignments")?;
    Ok(outcome?)
}

fn require_no_active_reassignment(admin: &Admin, topic: &str) -> Result<(), TestError> {
    let listed = wait_within(
        admin
            .list_partition_reassignments([TopicPartition::new(topic, 0)])
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "ListPartitionReassignments after exact-current alteration",
    )??;
    if !listed.reassignments().is_empty() {
        return Err(io::Error::other(
            "ListPartitionReassignments reported the completed no-op as active",
        )
        .into());
    }
    Ok(())
}

fn require_stable_assignment(
    initial: &TopicDescription,
    stable: &TopicDescription,
    topic: &str,
    replicas: &[i32],
) -> Result<(), TestError> {
    let initial_partition = require_single_healthy_partition(initial, topic)?;
    let stable_partition = require_single_healthy_partition(stable, topic)?;
    if stable.topic_id() != initial.topic_id()
        || stable_partition.replicas() != replicas
        || stable_partition.leader_id() != initial_partition.leader_id()
    {
        return Err(io::Error::other(
            "exact-current reassignment changed the topic, replica, or leader identity",
        )
        .into());
    }
    Ok(())
}

fn require_target(target: &TopicPartition, topic: &str, operation: &str) -> Result<(), TestError> {
    if target.topic() != topic || target.partition() != 0 {
        return Err(io::Error::other(format!(
            "{operation} returned unexpected target {}-{}",
            target.topic(),
            target.partition()
        ))
        .into());
    }
    Ok(())
}
