//! Controller, coordinator, topic-ID, and exact-broker Admin routing.

use std::{io, thread, time::Instant};

use kafkars::{
    Admin, DescribeConsumerGroupsResult, DescribeLogDirsResult, OffsetReset, Record, RetryAdvice,
};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, unique_name, wait_within, wait_within_for,
};

use super::{consume, nightly_support};

pub(super) fn controller_coordinator_and_exact_broker() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("admin-routes", 1)?;
    let admin = fixture.client.admin();
    let cluster = nightly_support::describe_cluster(&admin, "nightly DescribeCluster")?;
    if cluster.brokers().len() != 3 || cluster.controller_id().is_none() {
        return Err(
            io::Error::other("DescribeCluster omitted the three brokers or controller").into(),
        );
    }
    let topic = nightly_support::describe_topic(&admin, &fixture.topic)?;
    if topic.topic_id().is_none_or(|id| id == [0; 16]) {
        return Err(io::Error::other("DescribeTopics omitted the Kafka topic UUID").into());
    }

    let broker_ids = cluster
        .brokers()
        .iter()
        .map(kafkars::ClusterBroker::id)
        .collect::<Vec<_>>();
    let log_dirs = describe_log_dirs(&admin, &broker_ids)?;
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
    let mut consumer = consume::build_group(
        fixture
            .client
            .consumer(&group_id)
            .subscribe([&fixture.topic])
            .on_missing_offset(OffsetReset::Earliest),
        "admin coordinator member build",
    )?;
    let batch = wait_within(consumer.recv(), "admin coordinator group receive")??
        .ok_or_else(|| io::Error::other("coordinator group closed before delivery"))?;
    let groups = describe_consumer_groups(&admin, &group_id)?;
    let entries = groups.into_groups().into_entries();
    if entries.len() != 1 || entries[0].0 != group_id || entries[0].1.is_err() {
        return Err(io::Error::other("coordinator route did not describe the active group").into());
    }
    drop(batch);
    consume::close_group(consumer, "admin coordinator consumer close")?;
    nightly_support::close_producer(&producer, "admin coordinator producer close")?;
    fixture.finish()
}

fn describe_log_dirs(
    admin: &Admin,
    broker_ids: &[i32],
) -> Result<DescribeLogDirsResult, TestError> {
    let phase = "nightly exact-broker DescribeLogDirs";
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{phase} admission remained backpressured"),
            )
            .into());
        }
        let remaining = deadline.saturating_duration_since(now);
        let outcome = wait_within_for(
            admin
                .describe_log_dirs(broker_ids.iter().copied())
                .deadline_after(remaining)
                .submit(),
            phase,
            remaining,
        )?;
        match outcome {
            Ok(result) => return Ok(result),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn describe_consumer_groups(
    admin: &Admin,
    group_id: &str,
) -> Result<DescribeConsumerGroupsResult, TestError> {
    let phase = "coordinator-routed DescribeConsumerGroups";
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{phase} admission remained backpressured"),
            )
            .into());
        }
        let remaining = deadline.saturating_duration_since(now);
        let outcome = wait_within_for(
            admin
                .describe_consumer_groups([group_id])
                .deadline_after(remaining)
                .submit(),
            phase,
            remaining,
        )?;
        match outcome {
            Ok(result) => return Ok(result),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}
