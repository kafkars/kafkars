//! Shared exact-owner fixtures for partition-enrollment host scenarios.

use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use kafka_client_core::{
    Deadline, Moment, OperationId, ProducerRetryPolicy, TransactionEndOutcome, TransactionEpoch,
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionSequenceLease, TransactionalOwnerId, TransactionalProducerIdentity,
};

use crate::{
    clock::OperationDeadline, producer::materialization::TransactionalMaterializationBatch,
};

use super::{
    TransactionPartitionEnrollmentLimits, TransactionPartitionEnrollmentOwner,
    TransactionPartitionEnrollmentTerminal, TransactionPartitionEnrollmentTurn,
    port::TransactionPartitionEnrollmentPortFact,
};

pub(super) use super::host_port_test::{FakePort, RecordedRequest};

pub(super) fn terminal(
    epoch: TransactionEpoch,
    fact: TransactionPartitionEnrollmentPortFact,
) -> TransactionPartitionEnrollmentTerminal {
    let mut owner = owner(epoch);
    settle(&mut owner, epoch, fact);
    owner
        .take_terminal()
        .unwrap_or_else(|| panic!("one terminal expected"))
}

pub(super) fn settle(
    owner: &mut TransactionPartitionEnrollmentOwner,
    epoch: TransactionEpoch,
    fact: TransactionPartitionEnrollmentPortFact,
) {
    let _admission = owner
        .try_enroll(epoch, batch("orders", 2), deadline(20))
        .unwrap_or_else(|failure| panic!("valid admission: {:?}", failure.kind()));
    let mut port = FakePort::accepted(epoch, fact);
    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(port.discarded.load(Ordering::Acquire));
}

pub(super) fn owner(epoch: TransactionEpoch) -> TransactionPartitionEnrollmentOwner {
    owner_with_retry_policy(epoch, ProducerRetryPolicy::none())
}

pub(super) fn owner_with_retry_policy(
    epoch: TransactionEpoch,
    retry_policy: ProducerRetryPolicy,
) -> TransactionPartitionEnrollmentOwner {
    let identity = TransactionalProducerIdentity::try_new(41, 3)
        .unwrap_or_else(|| panic!("test identity must be valid"));
    let limits = TransactionPartitionEnrollmentLimits::try_new(4, 64)
        .unwrap_or_else(|| panic!("test limits must be valid"));
    let mut owner = TransactionPartitionEnrollmentOwner::try_start(
        "writer".into(),
        identity,
        limits,
        retry_policy,
    )
    .unwrap_or_else(|error| panic!("start enrollment owner: {error:?}"));
    owner
        .activate_epoch(epoch)
        .unwrap_or_else(|error| panic!("activate epoch: {error:?}"));
    owner
}

pub(super) fn batch(topic: &str, partition: i32) -> TransactionalMaterializationBatch {
    let identity = TransactionalProducerIdentity::try_new(41, 3)
        .unwrap_or_else(|| panic!("test identity must be valid"));
    batch_with_identity(topic, partition, identity)
}

pub(super) fn batch_with_identity(
    topic: &str,
    partition: i32,
    identity: TransactionalProducerIdentity,
) -> TransactionalMaterializationBatch {
    let sequence = TransactionSequenceLease::try_new(0, 1)
        .unwrap_or_else(|| panic!("test sequence must be valid"));
    TransactionalMaterializationBatch::new(
        topic.to_owned(),
        partition,
        Vec::new(),
        1_024,
        identity,
        sequence,
    )
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

pub(super) fn epochs() -> (TransactionEpoch, TransactionEpoch) {
    let owner = TransactionalOwnerId::from_raw(1);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let first = begin(&mut machine, owner);
    machine
        .apply(
            owner,
            TransactionLifecycleInput::Commit {
                epoch: first,
                operation_id: OperationId::from_raw(1),
            },
        )
        .unwrap_or_else(|error| panic!("commit transition: {error:?}"));
    machine
        .apply(
            owner,
            TransactionLifecycleInput::EndSettled {
                epoch: first,
                outcome: TransactionEndOutcome::Succeeded,
            },
        )
        .unwrap_or_else(|error| panic!("end settlement: {error:?}"));
    (first, begin(&mut machine, owner))
}

fn begin(
    machine: &mut TransactionLifecycleMachine,
    owner: TransactionalOwnerId,
) -> TransactionEpoch {
    let transition = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin transition: {error:?}"));
    let Some(TransactionLifecycleEffect::Began { epoch, .. }) = transition.into_effect() else {
        panic!("begin must emit epoch");
    };
    epoch
}
