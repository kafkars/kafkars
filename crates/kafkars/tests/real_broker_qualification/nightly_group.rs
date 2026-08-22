//! Classic and KIP-848 membership, rebalance, and committed-resume scenarios.

use std::{collections::BTreeSet, io};

use kafkars::{
    ClassicGroupAssignor, ConsumerGroupProtocol, GroupMembershipEpoch, OffsetReset, Record,
};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, client_builder_from_environment, ready_client, unique_name,
    wait_within,
};

use super::nightly_support;

pub(super) fn classic_join_and_cooperative_rebalance() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("classic-rebalance", 2)?;
    let group_id = unique_name("kafkars-cooperative");
    let first = cooperative_consumer(&fixture, &group_id)?;
    let first_epoch = nightly_support::poll_until(|| {
        let assignment = first.assignment().ok().flatten()?;
        (assignment_set(&assignment).len() == 2).then_some(assignment.assignment_epoch())
    })?;
    let second = cooperative_consumer(&fixture, &group_id)?;
    nightly_support::poll_until(|| {
        let first_assignment = first.assignment().ok().flatten()?;
        let second_assignment = second.assignment().ok().flatten()?;
        let first_set = assignment_set(&first_assignment);
        let second_set = assignment_set(&second_assignment);
        if first_set.len() == 1
            && second_set.len() == 1
            && first_set.is_disjoint(&second_set)
            && first_set.union(&second_set).count() == 2
            && first_assignment.assignment_epoch() > first_epoch
        {
            Some(())
        } else {
            None
        }
    })?;
    wait_within(first.try_close()?, "first cooperative close")??;
    wait_within(second.try_close()?, "second cooperative close")??;
    fixture.finish()
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
    let mut first = first_client
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
    let group_id = unique_name("kafkars-kip848");
    let expected = BTreeSet::from([(fixture.topic.clone(), 0), (fixture.topic.clone(), 1)]);
    let mut first = consumer_protocol_consumer(&fixture, &group_id)?;
    let first_member_epoch = nightly_support::poll_until(|| {
        let assignment = first.assignment().ok().flatten()?;
        let metadata = first.group_metadata().ok().flatten()?;
        if assignment_set(&assignment) != expected
            || assignment.assignment_epoch() != metadata.assignment_epoch()
        {
            return None;
        }
        match metadata.membership_epoch() {
            GroupMembershipEpoch::Consumer { member_epoch } if member_epoch > 0 => {
                Some(member_epoch)
            }
            GroupMembershipEpoch::Classic { .. } | GroupMembershipEpoch::Consumer { .. } => None,
        }
    })?;

    let mut second = consumer_protocol_consumer(&fixture, &group_id)?;
    let first_owns_partition_one = nightly_support::poll_until(|| {
        let first_assignment = first.assignment().ok().flatten()?;
        let first_metadata = first.group_metadata().ok().flatten()?;
        let second_assignment = second.assignment().ok().flatten()?;
        let second_metadata = second.group_metadata().ok().flatten()?;
        if first_assignment.assignment_epoch() != first_metadata.assignment_epoch()
            || second_assignment.assignment_epoch() != second_metadata.assignment_epoch()
        {
            return None;
        }
        let GroupMembershipEpoch::Consumer {
            member_epoch: current_first_epoch,
        } = first_metadata.membership_epoch()
        else {
            return None;
        };
        if !matches!(
            second_metadata.membership_epoch(),
            GroupMembershipEpoch::Consumer { member_epoch } if member_epoch > 0
        ) {
            return None;
        }
        let first_set = assignment_set(&first_assignment);
        let second_set = assignment_set(&second_assignment);
        let union = first_set
            .union(&second_set)
            .cloned()
            .collect::<BTreeSet<_>>();
        if first_set.len() != 1
            || second_set.len() != 1
            || !first_set.is_disjoint(&second_set)
            || union != expected
            || current_first_epoch <= first_member_epoch
        {
            return None;
        }
        Some(first_set.contains(&(fixture.topic.clone(), 1)))
    })?;

    if first_owns_partition_one {
        let batch = receive_kip848_partition_one(&mut first, &fixture.topic)?;
        wait_within(
            first.try_commit(batch.checkpoint(), OPERATION_TIMEOUT)?,
            "first KIP-848 member commit",
        )??;
    } else {
        let batch = receive_kip848_partition_one(&mut second, &fixture.topic)?;
        wait_within(
            second.try_commit(batch.checkpoint(), OPERATION_TIMEOUT)?,
            "second KIP-848 member commit",
        )??;
    }
    wait_within(second.try_close()?, "second KIP-848 member close")??;
    wait_within(first.try_close()?, "first KIP-848 member close")??;
    wait_within(producer.close(), "KIP-848 producer close")??;
    fixture.finish()
}

fn consumer_protocol_consumer(
    fixture: &nightly_support::Fixture,
    group_id: &str,
) -> Result<kafkars::Consumer, TestError> {
    Ok(fixture
        .client
        .consumer(group_id)
        .subscribe([&fixture.topic])
        .group_protocol(ConsumerGroupProtocol::Consumer)
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?)
}

fn receive_kip848_partition_one(
    consumer: &mut kafkars::Consumer,
    topic: &str,
) -> Result<kafkars::ConsumerBatch, TestError> {
    let batch = wait_within(consumer.recv(), "KIP-848 partition-one receive")??
        .ok_or_else(|| io::Error::other("KIP-848 partition-one owner closed before delivery"))?;
    let values = batch
        .records()
        .map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    if batch.topic() != topic || batch.partition() != 1 || values != [Some(b"kip848".to_vec())] {
        return Err(io::Error::other(
            "KIP-848 partition-one owner did not receive the exact seeded payload",
        )
        .into());
    }
    Ok(batch)
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
