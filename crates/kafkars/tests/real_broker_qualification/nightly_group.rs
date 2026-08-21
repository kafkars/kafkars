//! Classic and KIP-848 membership, rebalance, and committed-resume scenarios.

use std::{collections::BTreeSet, io};

use kafkars::{
    ClassicGroupAssignor, ConsumerGroupProtocol, GroupMembershipEpoch, OffsetReset, Record,
};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, unique_name, wait_within};

use super::nightly_support;

pub(super) fn classic_join_and_cooperative_rebalance() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("classic-rebalance", 2)?;
    let group_id = unique_name("kafkars-cooperative");
    let first = cooperative_consumer(&fixture, &group_id)?;
    let second = cooperative_consumer(&fixture, &group_id)?;
    let (first_assignment, second_assignment) = nightly_support::poll_until(|| {
        let first_assignment = first.assignment().ok().flatten()?;
        let second_assignment = second.assignment().ok().flatten()?;
        let first_set = assignment_set(&first_assignment);
        let second_set = assignment_set(&second_assignment);
        if first_set.is_disjoint(&second_set) && first_set.union(&second_set).count() == 2 {
            Some((first_assignment, second_assignment))
        } else {
            None
        }
    })?;
    if first_assignment.assignment_epoch() == second_assignment.assignment_epoch() {
        return Err(io::Error::other("separate members reused one local assignment fence").into());
    }
    wait_within(first.try_close()?, "first cooperative close")??;
    wait_within(second.try_close()?, "second cooperative close")??;
    fixture.finish()
}

pub(super) fn member_death_commit_and_resume() -> Result<(), TestError> {
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
    let mut first = fixture
        .client
        .consumer(&group_id)
        .subscribe([&fixture.topic])
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
    let batch = wait_within(first.recv(), "classic first member receive")??
        .ok_or_else(|| io::Error::other("first member closed before receiving"))?;
    let checkpoint = batch.checkpoint();
    wait_within(
        first.try_commit(checkpoint, OPERATION_TIMEOUT)?,
        "classic first member commit",
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
    let mut resumed = fixture
        .client
        .consumer(&group_id)
        .subscribe([&fixture.topic])
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
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
    wait_within(resumed.try_close()?, "classic resumed member close")??;
    wait_within(producer.close(), "classic resume producer close")??;
    fixture.finish()
}

pub(super) fn kip848_assignment_and_reconciliation() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("kip848", 2)?;
    let producer = fixture.client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(1)
                .value("kip848"),
        ),
        "KIP-848 seed delivery",
    )??;
    let mut consumer = fixture
        .client
        .consumer(unique_name("kafkars-kip848"))
        .subscribe([&fixture.topic])
        .group_protocol(ConsumerGroupProtocol::Consumer)
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
    let (assignment, metadata) = nightly_support::poll_until(|| {
        Some((
            consumer.assignment().ok().flatten()?,
            consumer.group_metadata().ok().flatten()?,
        ))
    })?;
    if assignment.partitions().len() != 2
        || !matches!(
            metadata.membership_epoch(),
            GroupMembershipEpoch::Consumer { member_epoch } if member_epoch > 0
        )
    {
        return Err(io::Error::other("KIP-848 did not install its typed assignment epoch").into());
    }
    let batch = wait_within(consumer.recv(), "KIP-848 receive")??
        .ok_or_else(|| io::Error::other("KIP-848 consumer closed before delivery"))?;
    wait_within(
        consumer.try_commit(batch.checkpoint(), OPERATION_TIMEOUT)?,
        "KIP-848 commit",
    )??;
    wait_within(consumer.try_close()?, "KIP-848 close")??;
    wait_within(producer.close(), "KIP-848 producer close")??;
    fixture.finish()
}

fn cooperative_consumer(
    fixture: &nightly_support::Fixture,
    group_id: &str,
) -> Result<kafkars::Consumer, TestError> {
    Ok(fixture
        .client
        .consumer(group_id)
        .subscribe([&fixture.topic])
        .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?)
}

fn assignment_set(assignment: &kafkars::ConsumerAssignment) -> BTreeSet<(String, i32)> {
    assignment
        .partitions()
        .iter()
        .map(|partition| (partition.topic().to_owned(), partition.partition()))
        .collect()
}
