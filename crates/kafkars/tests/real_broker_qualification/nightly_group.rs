//! Classic and KIP-848 membership, rebalance, and committed-resume scenarios.

use std::{collections::BTreeSet, io};

use kafkars::{
    ClassicGroupAssignor, Client, ConsumerGroupProtocol, ErrorKind, GroupMembershipEpoch,
    OffsetReset, Record,
};

use crate::real_broker_support::{
    TestError, client_builder_from_environment, ready_client, unique_name, wait_within,
};

use super::{consume, nightly_support};

pub(super) fn classic_cooperative_initial_assignment() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("classic-rebalance", 2)?;
    prove_partitions_writable(&fixture)?;
    let group_id = unique_name("kafkars-cooperative");
    let mut first = cooperative_consumer(&fixture.client, &fixture.topic, &group_id)?;
    nightly_support::poll_until(|| {
        let assignment = first.assignment().ok().flatten()?;
        (assignment_set(&assignment).len() == 2).then_some(())
    })?;
    prove_group_partitions_active(&mut first, &fixture.topic)?;
    consume::close_group(first, "first cooperative close")?;
    fixture.finish()
}

fn prove_group_partitions_active(
    consumer: &mut kafkars::Consumer,
    topic: &str,
) -> Result<(), TestError> {
    let expected = BTreeSet::from([0, 1]);
    let mut observed = BTreeSet::new();
    for _batch in 0..2 {
        let batch = wait_within(consumer.recv(), "cooperative partition readiness")??
            .ok_or_else(|| io::Error::other("cooperative member closed before delivery"))?;
        let partition = batch.partition();
        let expected_value = format!("group-ready-{partition}");
        if batch.topic() != topic
            || !batch
                .records()
                .any(|record| record.value() == Some(expected_value.as_bytes()))
        {
            return Err(io::Error::other("cooperative partition payload did not match").into());
        }
        observed.insert(partition);
        drop(batch);
        if observed == expected {
            return Ok(());
        }
    }
    Err(io::Error::other("cooperative member did not activate both partitions").into())
}

fn prove_partitions_writable(fixture: &nightly_support::Fixture) -> Result<(), TestError> {
    let producer = fixture.client.producer().build()?;
    for partition in 0..2 {
        wait_within(
            producer.send(
                Record::to(fixture.topic.as_str())
                    .partition(partition)
                    .value(format!("group-ready-{partition}")),
            ),
            "group partition readiness",
        )??;
    }
    nightly_support::close_producer(&producer, "group readiness producer close")
}

pub(super) fn member_shutdown_commit_and_resume() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("classic-resume", 1)?;
    let group_id = unique_name("kafkars-resume");
    let producer = fixture.client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("committed"),
        ),
        "classic resume seed",
    )??;
    let first_client =
        client_builder_from_environment("kafkars-nightly-classic-resume-first")?.build()?;
    ready_client(&first_client)?;
    let mut first = consume::build_group(
        first_client
            .consumer(&group_id)
            .subscribe([&fixture.topic])
            .on_missing_offset(OffsetReset::Earliest),
        "classic first member build",
    )?;
    let batch = wait_within(first.recv(), "classic first member receive")??
        .ok_or_else(|| io::Error::other("first member closed before receiving"))?;
    consume::commit_group(
        &mut first,
        batch.checkpoint(),
        "classic first member commit",
    )?;
    wait_within(
        first_client.shutdown(),
        "classic first member client shutdown",
    )??;
    drop(first);

    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("resumed"),
        ),
        "classic resumed delivery",
    )??;
    let mut resumed = consume::build_group(
        fixture
            .client
            .consumer(&group_id)
            .subscribe([&fixture.topic])
            .on_missing_offset(OffsetReset::Earliest),
        "classic resumed member build",
    )?;
    let resumed_batch = wait_within(resumed.recv(), "classic resumed member receive")??
        .ok_or_else(|| io::Error::other("replacement member closed before receiving"))?;
    let values = resumed_batch
        .records()
        .filter_map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    if !values.iter().any(|value| value == b"resumed")
        || values.iter().any(|value| value == b"committed")
    {
        return Err(
            io::Error::other("replacement member did not resume after committed offset").into(),
        );
    }
    drop(resumed_batch);
    consume::close_group(resumed, "classic resumed member close")?;
    nightly_support::close_producer(&producer, "classic resume producer close")?;
    fixture.finish()
}

pub(super) fn kip848_initial_assignment() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("kip848", 2)?;
    prove_partitions_writable(&fixture)?;
    let group_id = unique_name("kafkars-kip848");
    let expected = BTreeSet::from([(fixture.topic.clone(), 0), (fixture.topic.clone(), 1)]);
    let first = consumer_protocol_consumer(&fixture.client, &fixture.topic, &group_id)?;
    nightly_support::poll_until_result(|| {
        if let Some(error) = first.startup_error() {
            return Err(error.into());
        }
        let assignment = match first.assignment() {
            Ok(Some(assignment)) => assignment,
            Ok(None) => return Ok(None),
            Err(error) if error.kind() == ErrorKind::Backpressure => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let metadata = match first.group_metadata() {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return Ok(None),
            Err(error) if error.kind() == ErrorKind::Backpressure => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if assignment_set(&assignment) != expected
            || assignment.assignment_epoch() != metadata.assignment_epoch()
        {
            return Ok(None);
        }
        Ok(match metadata.membership_epoch() {
            GroupMembershipEpoch::Consumer { member_epoch } if member_epoch > 0 => Some(()),
            GroupMembershipEpoch::Classic { .. } | GroupMembershipEpoch::Consumer { .. } => None,
        })
    })?;
    consume::close_group(first, "first KIP-848 member close")?;
    fixture.finish()
}

fn consumer_protocol_consumer(
    client: &Client,
    topic: &str,
    group_id: &str,
) -> Result<kafkars::Consumer, TestError> {
    consume::build_group(
        client
            .consumer(group_id)
            .subscribe([topic])
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .on_missing_offset(OffsetReset::Earliest),
        "KIP-848 member build",
    )
}

fn cooperative_consumer(
    client: &Client,
    topic: &str,
    group_id: &str,
) -> Result<kafkars::Consumer, TestError> {
    consume::build_group(
        client
            .consumer(group_id)
            .subscribe([topic])
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
            .on_missing_offset(OffsetReset::Earliest),
        "cooperative member build",
    )
}

fn assignment_set(assignment: &kafkars::ConsumerAssignment) -> BTreeSet<(String, i32)> {
    assignment
        .partitions()
        .iter()
        .map(|partition| (partition.topic().to_owned(), partition.partition()))
        .collect()
}
