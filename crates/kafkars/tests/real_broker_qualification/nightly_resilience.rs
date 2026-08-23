//! Leader, broker, coordinator, and metadata recovery under managed disruption.

use std::{
    io, thread,
    time::{Duration, Instant},
};

use kafkars::{ConsumerFetchConfig, ErrorKind, OffsetReset, Record, StartPosition, TopicPartition};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, client_builder_from_environment, ready_client, unique_name,
    wait_within,
};

use super::{consume, nightly_control, nightly_control::BrokerGuard, nightly_support};

pub(super) fn producer_delivers_after_leader_movement() -> Result<(), TestError> {
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
    let replacement = nightly_support::poll_until(|| {
        let current = leader(&fixture).ok()?;
        (current != original_leader).then_some(current)
    })?;
    stopped.restore()?;
    if replacement == original_leader {
        return Err(io::Error::other("partition leader did not move").into());
    }
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("after-move"),
        ),
        "delivery after leader movement",
    )??;
    nightly_support::close_producer(&producer, "leader movement producer close")?;
    fixture.finish()
}

pub(super) fn consumer_recovers_fetch_across_leader_movement() -> Result<(), TestError> {
    let builder = client_builder_from_environment("kafkars-nightly-consumer-leader-movement")?
        .assigned_consumer_fetch(
            ConsumerFetchConfig::default().with_attempt_timeout(Duration::from_secs(60)),
        );
    let fixture = nightly_support::Fixture::from_builder(builder, "consumer-leader-movement", 1)?;
    let seed_client =
        client_builder_from_environment("kafkars-nightly-consumer-movement-seed")?.build()?;
    ready_client(&seed_client)?;
    let producer = seed_client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("before-move"),
        ),
        "consumer pre-movement seed",
    )??;
    nightly_support::close_producer(&producer, "consumer pre-movement producer close")?;
    wait_within(
        seed_client.shutdown(),
        "consumer movement seed client shutdown",
    )??;
    let mut consumer = fixture.client.assigned_consumer().build()?;
    consume::retry_assigned_control("consumer movement assignment", || {
        consumer.try_replace_assignment(
            [TopicPartition::new(&fixture.topic, 0).start_at(StartPosition::Beginning)],
            OPERATION_TIMEOUT,
        )
    })?;
    require_direct_value(&mut consumer, b"before-move")?;

    let original_leader = leader(&fixture)?;
    let stopped = BrokerGuard::stop(original_leader)?;
    let replacement = nightly_support::poll_until(|| {
        let current = leader(&fixture).ok()?;
        (current != original_leader).then_some(current)
    })?;
    if replacement == original_leader {
        return Err(io::Error::other("consumer partition leader did not move").into());
    }
    wait_for_writable_isr(&fixture)?;
    let post_client =
        client_builder_from_environment("kafkars-nightly-consumer-movement-post")?.build()?;
    ready_client(&post_client)?;
    let producer = post_client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("after-move"),
        ),
        "consumer post-movement seed",
    )??;
    require_direct_value(&mut consumer, b"after-move")?;
    if leader(&fixture)? != replacement {
        return Err(io::Error::other(
            "replacement broker did not remain leader through post-movement fetch",
        )
        .into());
    }
    stopped.restore()?;
    consume::close_assigned(&mut consumer, "consumer movement close")?;
    nightly_support::close_producer(&producer, "consumer movement producer close")?;
    wait_within(
        post_client.shutdown(),
        "consumer movement producer client shutdown",
    )??;
    fixture.finish()
}

pub(super) fn cluster_usable_after_broker_restart() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("broker-restart", 1)?;
    let admin = fixture.client.admin();
    let cluster = nightly_support::describe_cluster(&admin, "pre-restart DescribeCluster")?;
    let broker_id = cluster
        .brokers()
        .last()
        .ok_or_else(|| io::Error::other("cluster description had no broker"))?
        .id();
    nightly_control::restart(broker_id)?;
    ready_client(&fixture.client)?;
    let refreshed = nightly_support::describe_cluster(&admin, "post-restart DescribeCluster")?;
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
    nightly_support::close_producer(&producer, "broker restart producer close")?;
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
    let mut consumer = consume::build_group(
        fixture
            .client
            .consumer(&group_id)
            .subscribe([&fixture.topic])
            .on_missing_offset(OffsetReset::Earliest),
        "coordinator-restart member build",
    )?;
    let first = wait_within(consumer.recv(), "pre-loss group receive")??
        .ok_or_else(|| io::Error::other("group closed before coordinator disruption"))?;
    consume::commit_group(&mut consumer, first.checkpoint(), "pre-loss group commit")?;
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
    drop(recovered);
    consume::close_group(consumer, "coordinator-restart consumer close")?;
    nightly_support::close_producer(&producer, "coordinator-restart producer close")?;
    fixture.finish()
}

fn leader(fixture: &nightly_support::Fixture) -> Result<i32, TestError> {
    nightly_support::describe_topic(&fixture.client.admin(), &fixture.topic)?
        .partitions()
        .first()
        .and_then(kafkars::TopicPartitionDescription::leader_id)
        .ok_or_else(|| io::Error::other("qualification partition has no leader").into())
}

fn wait_for_writable_isr(fixture: &nightly_support::Fixture) -> Result<(), TestError> {
    nightly_support::poll_until(|| {
        let topic =
            nightly_support::describe_topic(&fixture.client.admin(), &fixture.topic).ok()?;
        let partition = topic.partitions().first()?;
        (partition.in_sync_replicas().len() >= 2).then_some(())
    })
}

fn require_direct_value(
    consumer: &mut kafkars::AssignedConsumer,
    expected: &[u8],
) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match consumer.try_take_batch() {
            Ok(Some(batch)) => {
                let found = batch
                    .records()
                    .any(|record| record.value() == Some(expected));
                drop(batch);
                if found {
                    return Ok(());
                }
            }
            Ok(None) => {}
            Err(error) if error.kind() == ErrorKind::Backpressure => {}
            Err(error) => return Err(error.into()),
        }
        match consumer.try_take_event() {
            Ok(Some(event)) => {
                return Err(io::Error::other(format!(
                    "consumer failed during leader movement: {event:?}"
                ))
                .into());
            }
            Ok(None) => {}
            Err(error) if error.kind() == ErrorKind::Backpressure => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "leader movement consumer receive did not complete within 30s",
            )
            .into());
        }
        thread::sleep(Duration::from_millis(1));
    }
}
