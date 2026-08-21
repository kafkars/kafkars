//! Controller, coordinator, topic-ID, and exact-broker Admin routing.

use std::io;

use kafkars::{OffsetReset, Record};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, unique_name, wait_within};

use super::nightly_support;

pub(super) fn controller_coordinator_and_exact_broker() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("admin-routes", 1)?;
    let admin = fixture.client.admin();
    let cluster = wait_within(
        admin
            .describe_cluster()
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "nightly DescribeCluster",
    )??;
    if cluster.brokers().len() != 3 || cluster.controller_id().is_none() {
        return Err(
            io::Error::other("DescribeCluster omitted the three brokers or controller").into(),
        );
    }
    let topic = nightly_support::describe_topic(&admin, &fixture.topic)?;
    if topic.topic_id().is_none_or(|id| id == [0; 16]) {
        return Err(io::Error::other("DescribeTopics omitted the Kafka topic UUID").into());
    }

    let broker_ids = cluster.brokers().iter().map(kafkars::ClusterBroker::id);
    let log_dirs = wait_within(
        admin
            .describe_log_dirs(broker_ids)
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "nightly exact-broker DescribeLogDirs",
    )??;
    let broker_results = log_dirs.into_brokers().into_entries();
    if broker_results.len() != 3 {
        return Err(io::Error::other("DescribeLogDirs omitted an exact broker result").into());
    }
    for (_broker_id, outcome) in broker_results {
        let directories = outcome?;
        if directories.entries().is_empty() {
            return Err(io::Error::other("an exact broker returned no log directories").into());
        }
        for (_path, directory) in directories.into_entries() {
            directory?;
        }
    }

    let producer = fixture.client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("coordinator"),
        ),
        "admin coordinator seed",
    )??;
    let group_id = unique_name("kafkars-admin-coordinator");
    let mut consumer = fixture
        .client
        .consumer(&group_id)
        .subscribe([&fixture.topic])
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
    let _batch = wait_within(consumer.recv(), "admin coordinator group receive")??
        .ok_or_else(|| io::Error::other("coordinator group closed before delivery"))?;
    let groups = wait_within(
        admin
            .describe_consumer_groups([&group_id])
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "coordinator-routed DescribeConsumerGroups",
    )??;
    let entries = groups.into_groups().into_entries();
    if entries.len() != 1 || entries[0].0 != group_id || entries[0].1.is_err() {
        return Err(io::Error::other("coordinator route did not describe the active group").into());
    }
    wait_within(consumer.try_close()?, "admin coordinator consumer close")??;
    wait_within(producer.close(), "admin coordinator producer close")??;
    fixture.finish()
}
