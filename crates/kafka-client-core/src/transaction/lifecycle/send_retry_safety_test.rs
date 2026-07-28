//! Non-retryable failure and terminal lifecycle replacement exclusions.

use super::send_retry_test_support::{
    attempt_failed, broker_failure, prepare, retry_machine, send_identity,
};
use super::test_support::{accept, begin, effect, owner, send};
use super::{
    TransactionLifecycleEffect, TransactionLifecycleInput, TransactionLifecycleState,
    TransactionSendAttempt, TransactionSendAttemptFailure, TransactionSendOutcome,
};
use crate::ProducerBrokerFailureKind;

#[test]
fn nonrouting_fencing_uncertain_and_expired_failures_have_no_hidden_retry() {
    let cases = [
        broker_failure(ProducerBrokerFailureKind::AccessRejected, 29),
        broker_failure(ProducerBrokerFailureKind::ProducerFenced, 90),
        TransactionSendAttemptFailure::Uncertain,
    ];
    for (index, failure) in cases.into_iter().enumerate() {
        let owner_id = owner(30 + index as u64);
        let mut machine = retry_machine(owner_id, 2, 10);
        let epoch = begin(&mut machine, owner_id);
        let send_id = send(1);
        accept(&mut machine, owner_id, epoch, send_id);
        prepare(&mut machine, owner_id, epoch, send_id, send_identity(100));

        let denied = attempt_failed(
            &mut machine,
            owner_id,
            epoch,
            send_id,
            TransactionSendAttempt::initial(),
            5,
            failure,
        )
        .unwrap_or_else(|error| panic!("nonretryable failure: {error}"));
        assert_eq!(denied.into_effect(), None);

        let still_initial = attempt_failed(
            &mut machine,
            owner_id,
            epoch,
            send_id,
            TransactionSendAttempt::initial(),
            5,
            broker_failure(ProducerBrokerFailureKind::Routing, 6),
        )
        .unwrap_or_else(|error| panic!("routing after nonretryable failure: {error}"));
        assert!(still_initial.into_effect().is_some());
    }

    let owner_id = owner(40);
    let mut machine = retry_machine(owner_id, 1, 10);
    let epoch = begin(&mut machine, owner_id);
    let send_id = send(1);
    accept(&mut machine, owner_id, epoch, send_id);
    prepare(&mut machine, owner_id, epoch, send_id, send_identity(15));
    let no_window = attempt_failed(
        &mut machine,
        owner_id,
        epoch,
        send_id,
        TransactionSendAttempt::initial(),
        5,
        broker_failure(ProducerBrokerFailureKind::Routing, 6),
    )
    .unwrap_or_else(|error| panic!("deadline-capped failure: {error}"));
    assert_eq!(no_window.into_effect(), None);
}

#[test]
fn abort_required_and_fatal_lifecycles_never_replace_remaining_sends() {
    let owner_id = owner(50);
    let mut aborting = retry_machine(owner_id, 1, 5);
    let epoch = begin(&mut aborting, owner_id);
    accept(&mut aborting, owner_id, epoch, send(1));
    accept(&mut aborting, owner_id, epoch, send(2));
    prepare(&mut aborting, owner_id, epoch, send(2), send_identity(100));
    assert_eq!(
        effect(
            &mut aborting,
            owner_id,
            TransactionLifecycleInput::SendSettled {
                epoch,
                send_id: send(1),
                outcome: TransactionSendOutcome::AbortRequired,
            },
        ),
        TransactionLifecycleEffect::AbortRequired { owner_id, epoch }
    );
    assert_eq!(aborting.state(), TransactionLifecycleState::AbortRequired);
    assert_eq!(
        attempt_failed(
            &mut aborting,
            owner_id,
            epoch,
            send(2),
            TransactionSendAttempt::initial(),
            1,
            broker_failure(ProducerBrokerFailureKind::Routing, 6),
        )
        .unwrap_or_else(|error| panic!("abort-required route failure: {error}"))
        .into_effect(),
        None
    );

    let owner_id = owner(51);
    let mut fatal = retry_machine(owner_id, 1, 5);
    let epoch = begin(&mut fatal, owner_id);
    accept(&mut fatal, owner_id, epoch, send(1));
    accept(&mut fatal, owner_id, epoch, send(2));
    prepare(&mut fatal, owner_id, epoch, send(2), send_identity(100));
    let fatal_effect = effect(
        &mut fatal,
        owner_id,
        TransactionLifecycleInput::SendSettled {
            epoch,
            send_id: send(1),
            outcome: TransactionSendOutcome::Fatal,
        },
    );
    assert!(matches!(
        fatal_effect,
        TransactionLifecycleEffect::EnterFatal { .. }
    ));
    assert_eq!(
        attempt_failed(
            &mut fatal,
            owner_id,
            epoch,
            send(2),
            TransactionSendAttempt::initial(),
            1,
            broker_failure(ProducerBrokerFailureKind::Routing, 6),
        )
        .unwrap_or_else(|error| panic!("fatal route failure: {error}"))
        .into_effect(),
        None
    );
}
