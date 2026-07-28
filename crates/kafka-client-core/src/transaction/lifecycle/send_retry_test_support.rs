//! Shared fixtures for bounded transactional-send replacement tests.

use core::num::NonZeroI16;

use super::{
    TransactionEpoch, TransactionLifecycleInput, TransactionLifecycleMachine,
    TransactionLifecycleMachineError, TransactionLifecycleTransition, TransactionSendAttempt,
    TransactionSendAttemptFailure, TransactionSendId, TransactionSendIdentity,
    TransactionSequenceLease,
};
use crate::{
    Deadline, Moment, PartitionIndex, ProducerBrokerFailure, ProducerBrokerFailureKind,
    ProducerRetryPolicy, TopicId, TransactionPartition, TransactionalOwnerId,
    TransactionalProducerIdentity,
};

pub(super) fn retry_machine(
    owner_id: TransactionalOwnerId,
    retries: u32,
    backoff: u64,
) -> TransactionLifecycleMachine {
    let policy = ProducerRetryPolicy::try_fixed(retries, backoff)
        .unwrap_or_else(|error| panic!("test retry policy: {error}"));
    TransactionLifecycleMachine::with_send_retry_policy(owner_id, policy)
}

pub(super) fn send_identity(deadline: u64) -> TransactionSendIdentity {
    TransactionSendIdentity::new(
        TransactionalProducerIdentity::try_new(41, 3)
            .unwrap_or_else(|| panic!("test producer identity is valid")),
        TransactionPartition::new(TopicId::from_raw(7), PartitionIndex::from_raw(2)),
        TransactionSequenceLease::try_new(19, 2)
            .unwrap_or_else(|| panic!("test sequence lease is valid")),
        Deadline::from_tick(deadline),
    )
}

pub(super) fn prepare(
    machine: &mut TransactionLifecycleMachine,
    owner_id: TransactionalOwnerId,
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    identity: TransactionSendIdentity,
) {
    let transition = machine
        .apply(
            owner_id,
            TransactionLifecycleInput::SendPrepared {
                epoch,
                send_id,
                identity,
            },
        )
        .unwrap_or_else(|error| panic!("send preparation: {error}"));
    assert_eq!(transition.into_effect(), None);
}

pub(super) fn broker_failure(
    kind: ProducerBrokerFailureKind,
    code: i16,
) -> TransactionSendAttemptFailure {
    TransactionSendAttemptFailure::Broker(ProducerBrokerFailure::new(
        kind,
        NonZeroI16::new(code).unwrap_or_else(|| panic!("test broker code is nonzero")),
    ))
}

pub(super) fn attempt_failed(
    machine: &mut TransactionLifecycleMachine,
    owner_id: TransactionalOwnerId,
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    attempt: TransactionSendAttempt,
    now: u64,
    failure: TransactionSendAttemptFailure,
) -> Result<TransactionLifecycleTransition, TransactionLifecycleMachineError> {
    machine.apply(
        owner_id,
        TransactionLifecycleInput::SendAttemptFailed {
            epoch,
            send_id,
            attempt,
            now: Moment::from_tick(now),
            failure,
        },
    )
}
