//! Leader, broker, coordinator, and metadata recovery under managed disruption.

use std::{io, time::Duration};

use kafkars::{OffsetReset, Record};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, client_builder_from_environment, unique_name, wait_within,
};

use super::{nightly_control, nightly_control::BrokerGuard, nightly_support};

pub(super) fn producer_delivers_across_leader_movement() -> Result<(), TestError> {
    let builder = client_builder_from_environment("kafkars-nightly-leader-movement")?
        .producer_retry(10, Duration::from_millis(200));
    let fixture = nightly_support::Fixture::from_builder(builder, "producer-leader-movement", 1)?;
    let producer = fixture.client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("before-move"),
        ),
        "pre-movement delivery",
    )??;
    let original_leader = leader(&fixture)?;
    let stopped = BrokerGuard::stop(original_leader)?;
    let delivery = producer.send(
        Record::to(fixture.topic.as_str())
            .partition(0)
            .value("during-move"),
    );
    let replacement = nightly_support::poll_until(|| {
        let current = leader(&fixture).ok()?;
        (current != original_leader).then_some(current)
    })?;
    wait_within(delivery, "delivery across leader movement")??;
    stopped.restore()?;
    if replacement == original_leader {
        return Err(io::Error::other("partition leader did not move").into());
    }
    wait_within(producer.close(), "leader movement producer close")??;
    fixture.finish()
}

pub(super) fn cluster_usable_after_broker_restart() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("broker-restart", 1)?;
    let admin = fixture.client.admin();
    let cluster = wait_within(
        admin
            .describe_cluster()
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "pre-restart DescribeCluster",
    )??;
    let broker_id = cluster
        .brokers()
        .last()
        .ok_or_else(|| io::Error::other("cluster description had no broker"))?
        .id();
    nightly_control::restart(broker_id)?;
    wait_within(fixture.client.ready(), "readiness after broker restart")??;
    let refreshed = wait_within(
        admin
            .describe_cluster()
            .deadline_after(OPERATION_TIMEOUT)
            .submit(),
        "post-restart DescribeCluster",
    )??;
    if refreshed.brokers().len() != 3 {
        return Err(io::Error::other("metadata refresh lost a broker after restart").into());
    }
    let producer = fixture.client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("after-restart"),
        ),
        "post-restart delivery",
    )??;
    wait_within(producer.close(), "broker restart producer close")??;
    fixture.finish()
}

pub(super) fn group_usable_after_coordinator_restart() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("coordinator-restart", 1)?;
    let producer = fixture.client.producer().build()?;
    let group_id = unique_name("kafkars-coordinator-restart");
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("before-loss"),
        ),
        "coordinator-restart seed",
    )??;
    let mut consumer = fixture
        .client
        .consumer(&group_id)
        .subscribe([&fixture.topic])
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
    let first = wait_within(consumer.recv(), "pre-loss group receive")??
        .ok_or_else(|| io::Error::other("group closed before coordinator disruption"))?;
    wait_within(
        consumer.try_commit(first.checkpoint(), OPERATION_TIMEOUT)?,
        "pre-loss group commit",
    )??;
    nightly_control::restart_coordinator(&group_id)?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("after-loss"),
        ),
        "post-loss delivery",
    )??;
    let recovered = wait_within(consumer.recv(), "post-loss group receive")??
        .ok_or_else(|| io::Error::other("group closed after coordinator disruption"))?;
    if !recovered
        .records()
        .any(|record| record.value() == Some(b"after-loss".as_slice()))
    {
        return Err(io::Error::other("group omitted record after coordinator restart").into());
    }
    wait_within(consumer.try_close()?, "coordinator-restart consumer close")??;
    wait_within(producer.close(), "coordinator-restart producer close")??;
    fixture.finish()
}

fn leader(fixture: &nightly_support::Fixture) -> Result<i32, TestError> {
    nightly_support::describe_topic(&fixture.client.admin(), &fixture.topic)?
        .partitions()
        .first()
        .and_then(kafkars::TopicPartitionDescription::leader_id)
        .ok_or_else(|| io::Error::other("qualification partition has no leader").into())
}
