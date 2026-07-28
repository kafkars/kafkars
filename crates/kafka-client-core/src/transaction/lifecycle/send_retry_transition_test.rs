//! Bounded replacement generation and immutable send-authority scenarios.

use super::send_retry_test_support::{
    attempt_failed, broker_failure, prepare, retry_machine, send_identity,
};
use super::test_support::{accept, begin, effect, owner, send};
use super::{
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleMachineError,
    TransactionLifecycleState, TransactionSendAttempt, TransactionSendOutcome,
};
use crate::{Deadline, PartitionIndex, ProducerBrokerFailureKind, TopicId};

#[test]
fn routing_replacement_is_bounded_and_preserves_exact_send_authority() {
    let owner_id = owner(20);
    let mut machine = retry_machine(owner_id, 2, 10);
    let epoch = begin(&mut machine, owner_id);
    let send_id = send(4);
    let identity = send_identity(100);
    accept(&mut machine, owner_id, epoch, send_id);
    prepare(&mut machine, owner_id, epoch, send_id, identity);

    let first = attempt_failed(
        &mut machine,
        owner_id,
        epoch,
        send_id,
        TransactionSendAttempt::initial(),
        5,
        broker_failure(ProducerBrokerFailureKind::Routing, 6),
    )
    .unwrap_or_else(|error| panic!("first routing failure: {error}"))
    .into_effect()
    .unwrap_or_else(|| panic!("first routing failure authorizes replacement"));
    let TransactionLifecycleEffect::ReplaceSendAttempt {
        owner_id: effect_owner,
        epoch: effect_epoch,
        send_id: effect_send,
        previous,
        replacement: first_replacement,
        identity: effect_identity,
        not_before,
    } = first
    else {
        panic!("first routing failure emits one replacement");
    };
    assert_eq!(effect_owner, owner_id);
    assert_eq!(effect_epoch, epoch);
    assert_eq!(effect_send, send_id);
    assert_eq!(previous, TransactionSendAttempt::initial());
    assert_eq!(first_replacement.get(), 1);
    assert_eq!(effect_identity, identity);
    assert_eq!(not_before, Deadline::from_tick(15));
    assert_eq!(identity.producer().producer_id(), 41);
    assert_eq!(identity.producer().producer_epoch(), 3);
    assert_eq!(identity.partition().topic_id(), TopicId::from_raw(7));
    assert_eq!(
        identity.partition().partition(),
        PartitionIndex::from_raw(2)
    );
    assert_eq!(identity.sequence().base_sequence(), 19);
    assert_eq!(identity.sequence().record_count(), 2);
    assert_eq!(identity.deadline(), Deadline::from_tick(100));

    let second = attempt_failed(
        &mut machine,
        owner_id,
        epoch,
        send_id,
        first_replacement,
        20,
        broker_failure(ProducerBrokerFailureKind::Routing, 6),
    )
    .unwrap_or_else(|error| panic!("second routing failure: {error}"))
    .into_effect()
    .unwrap_or_else(|| panic!("second routing failure authorizes replacement"));
    let TransactionLifecycleEffect::ReplaceSendAttempt {
        previous,
        replacement: second_replacement,
        identity: actual_identity,
        not_before,
        ..
    } = second
    else {
        panic!("second routing failure emits one replacement");
    };
    assert_eq!(previous, first_replacement);
    assert_eq!(second_replacement.get(), 2);
    assert_eq!(actual_identity, identity);
    assert_eq!(not_before, Deadline::from_tick(30));

    let exhausted = attempt_failed(
        &mut machine,
        owner_id,
        epoch,
        send_id,
        second_replacement,
        30,
        broker_failure(ProducerBrokerFailureKind::Routing, 6),
    )
    .unwrap_or_else(|error| panic!("bounded routing failure: {error}"));
    assert_eq!(exhausted.into_effect(), None);
    assert_eq!(machine.outstanding_send_count(), 1);
    assert_eq!(
        effect(
            &mut machine,
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id,
                outcome: TransactionSendOutcome::AbortRequired,
            },
        ),
        TransactionLifecycleEffect::AbortRequired { owner_id, epoch }
    );
    assert_eq!(machine.outstanding_send_count(), 0);
    assert_eq!(machine.state(), TransactionLifecycleState::AbortRequired);
}

#[test]
fn stale_attempt_and_unprepared_shape_cannot_authorize_replacement() {
    let owner_id = owner(21);
    let mut machine = retry_machine(owner_id, 1, 5);
    let epoch = begin(&mut machine, owner_id);
    let send_id = send(1);
    accept(&mut machine, owner_id, epoch, send_id);

    assert_eq!(
        attempt_failed(
            &mut machine,
            owner_id,
            epoch,
            send_id,
            TransactionSendAttempt::initial(),
            1,
            broker_failure(ProducerBrokerFailureKind::Routing, 6),
        ),
        Err(TransactionLifecycleMachineError::SendNotPrepared { send_id })
    );

    let identity = send_identity(100);
    prepare(&mut machine, owner_id, epoch, send_id, identity);
    let authorized = attempt_failed(
        &mut machine,
        owner_id,
        epoch,
        send_id,
        TransactionSendAttempt::initial(),
        1,
        broker_failure(ProducerBrokerFailureKind::Routing, 6),
    )
    .unwrap_or_else(|error| panic!("routing replacement: {error}"));
    let Some(TransactionLifecycleEffect::ReplaceSendAttempt { replacement, .. }) =
        authorized.into_effect()
    else {
        panic!("routing failure emits one replacement");
    };
    assert_eq!(
        attempt_failed(
            &mut machine,
            owner_id,
            epoch,
            send_id,
            TransactionSendAttempt::initial(),
            2,
            broker_failure(ProducerBrokerFailureKind::Routing, 6),
        ),
        Err(TransactionLifecycleMachineError::SendAttemptMismatch {
            expected: replacement,
            supplied: TransactionSendAttempt::initial(),
        })
    );
    assert_eq!(machine.outstanding_send_count(), 1);
}
