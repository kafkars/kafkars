//! KIP-848 multi-member redistribution, fetch, commit, shutdown, and resume proof.

use std::{
    collections::BTreeSet,
    io, thread,
    time::{Duration, Instant},
};

use kafkars::{
    Client, Consumer, ConsumerEvent, ConsumerGroupProtocol, ErrorKind, GroupMembershipEpoch,
    OffsetReset, Producer, Record, RetryAdvice,
};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, client_builder_from_environment, ready_client, unique_name,
    wait_within,
};

use super::{consume, nightly_support};

#[derive(Debug)]
struct ObservedAssignment {
    partitions: BTreeSet<(String, i32)>,
    assignment_epoch: u64,
    member_epoch: i32,
}

pub(super) fn multi_member_commit_shutdown_and_resume() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("kip848", 2)?;
    let producer = fixture.client.producer().build()?;
    produce_partition_values(&producer, &fixture.topic, "initial")?;
    let group_id = unique_name("kafkars-kip848");
    let expected = expected_partitions(&fixture.topic);
    let mut first = consumer_protocol_consumer(&fixture.client, &fixture.topic, &group_id)?;
    let solo = wait_for_assignment(&mut first, &expected, None, "KIP-848 solo assignment")?;

    let second_client =
        client_builder_from_environment("kafkars-nightly-kip848-second")?.build()?;
    ready_client(&second_client)?;
    let mut second = consumer_protocol_consumer(&second_client, &fixture.topic, &group_id)?;
    let (first_split, second_split) = wait_for_split(&mut first, &mut second, &expected, &solo)?;

    receive_and_commit(
        &mut first,
        &first_split.partitions,
        "initial",
        None,
        "first KIP-848 initial receive",
    )?;
    receive_and_commit(
        &mut second,
        &second_split.partitions,
        "initial",
        None,
        "second KIP-848 initial receive",
    )?;
    consume::close_group(second, "second KIP-848 member close")?;
    wait_within(second_client.shutdown(), "second KIP-848 client shutdown")??;

    let reclaimed = wait_for_assignment(
        &mut first,
        &expected,
        Some(first_split.assignment_epoch),
        "KIP-848 surviving-member reclaim",
    )?;
    if reclaimed.assignment_epoch <= first_split.assignment_epoch {
        return Err(io::Error::other("surviving member did not advance after shutdown").into());
    }

    produce_partition_values(&producer, &fixture.topic, "resumed")?;
    receive_and_commit(
        &mut first,
        &expected,
        "resumed",
        Some("initial"),
        "surviving KIP-848 resumed receive",
    )?;
    nightly_support::close_producer(&producer, "KIP-848 producer close")?;
    consume::close_group(first, "surviving KIP-848 member close")?;
    fixture.finish()
}

fn wait_for_split(
    first: &mut Consumer,
    second: &mut Consumer,
    expected: &BTreeSet<(String, i32)>,
    solo: &ObservedAssignment,
) -> Result<(ObservedAssignment, ObservedAssignment), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    let mut last = String::from("no observations");
    loop {
        complete_rebalance_events(first)?;
        complete_rebalance_events(second)?;
        if let (Some(first_state), Some(second_state)) =
            (observe_assignment(first)?, observe_assignment(second)?)
        {
            let union = first_state
                .partitions
                .union(&second_state.partitions)
                .cloned()
                .collect::<BTreeSet<_>>();
            let redistributed = first_state.partitions.len() == 1
                && second_state.partitions.len() == 1
                && first_state.partitions.is_disjoint(&second_state.partitions)
                && union == *expected
                && first_state.assignment_epoch > solo.assignment_epoch
                && first_state.member_epoch >= solo.member_epoch;
            if redistributed {
                return Ok((first_state, second_state));
            }
            last = format!("first={first_state:?}, second={second_state:?}");
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("KIP-848 two-member redistribution: {last}"),
            )
            .into());
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn consumer_protocol_consumer(
    client: &Client,
    topic: &str,
    group_id: &str,
) -> Result<Consumer, TestError> {
    consume::build_group(
        client
            .consumer(group_id)
            .subscribe([topic])
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .on_missing_offset(OffsetReset::Earliest),
        "KIP-848 member build",
    )
}

fn wait_for_assignment(
    consumer: &mut Consumer,
    expected: &BTreeSet<(String, i32)>,
    after_assignment_epoch: Option<u64>,
    phase: &str,
) -> Result<ObservedAssignment, TestError> {
    nightly_support::poll_until_result(phase, || {
        complete_rebalance_events(consumer)?;
        let Some(state) = observe_assignment(consumer)? else {
            return Ok(None);
        };
        let advanced =
            after_assignment_epoch.is_none_or(|assignment| state.assignment_epoch > assignment);
        Ok((state.partitions == *expected && advanced).then_some(state))
    })
}

fn complete_rebalance_events(consumer: &mut Consumer) -> Result<(), TestError> {
    for _event in 0..4 {
        let event = match consumer.try_take_event() {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::Backpressure => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let ConsumerEvent::PartitionsRevoking(mut revocation) = event else {
            continue;
        };
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        loop {
            match revocation.complete() {
                Ok(()) => break,
                Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::other(
                            "KIP-848 revocation completion remained backpressured",
                        )
                        .into());
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

fn observe_assignment(consumer: &Consumer) -> Result<Option<ObservedAssignment>, TestError> {
    if let Some(error) = consumer.startup_error() {
        return Err(error.into());
    }
    let Some(assignment) = pending_backpressure(consumer.assignment())? else {
        return Ok(None);
    };
    let Some(metadata) = pending_backpressure(consumer.group_metadata())? else {
        return Ok(None);
    };
    if assignment.assignment_epoch() != metadata.assignment_epoch() {
        return Ok(None);
    }
    let GroupMembershipEpoch::Consumer { member_epoch } = metadata.membership_epoch() else {
        return Err(io::Error::other("KIP-848 member exposed a classic epoch").into());
    };
    if member_epoch <= 0 {
        return Ok(None);
    }
    Ok(Some(ObservedAssignment {
        partitions: assignment
            .partitions()
            .iter()
            .map(|partition| (partition.topic().to_owned(), partition.partition()))
            .collect(),
        assignment_epoch: assignment.assignment_epoch(),
        member_epoch,
    }))
}

fn pending_backpressure<T>(
    result: Result<Option<T>, kafkars::KafkaError>,
) -> Result<Option<T>, TestError> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == ErrorKind::Backpressure => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn produce_partition_values(
    producer: &Producer,
    topic: &str,
    prefix: &str,
) -> Result<(), TestError> {
    for partition in 0..2 {
        wait_within(
            producer.send(
                Record::to(topic)
                    .partition(partition)
                    .value(format!("{prefix}-{partition}")),
            ),
            "KIP-848 partition production",
        )??;
    }
    Ok(())
}

fn receive_and_commit(
    consumer: &mut Consumer,
    expected: &BTreeSet<(String, i32)>,
    prefix: &str,
    forbidden_prefix: Option<&str>,
    phase: &str,
) -> Result<(), TestError> {
    let mut observed = BTreeSet::new();
    while observed.len() < expected.len() {
        let batch = wait_within(consumer.recv(), phase)??
            .ok_or_else(|| io::Error::other("KIP-848 member closed before delivery"))?;
        let partition = (batch.topic().to_owned(), batch.partition());
        if !expected.contains(&partition) {
            return Err(io::Error::other("KIP-848 member received an unowned partition").into());
        }
        let expected_value = format!("{prefix}-{}", batch.partition());
        let forbidden_value =
            forbidden_prefix.map(|prefix| format!("{prefix}-{}", batch.partition()));
        let values = batch
            .records()
            .filter_map(|record| record.value().map(<[u8]>::to_vec))
            .collect::<Vec<_>>();
        if !values
            .iter()
            .any(|value| value == expected_value.as_bytes())
            || forbidden_value
                .as_ref()
                .is_some_and(|forbidden| values.iter().any(|value| value == forbidden.as_bytes()))
        {
            return Err(
                io::Error::other("KIP-848 delivery did not resume at committed offsets").into(),
            );
        }
        consume::commit_group(consumer, batch.checkpoint(), "KIP-848 member commit")?;
        observed.insert(partition);
    }
    Ok(())
}

fn expected_partitions(topic: &str) -> BTreeSet<(String, i32)> {
    BTreeSet::from([(topic.to_owned(), 0), (topic.to_owned(), 1)])
}
