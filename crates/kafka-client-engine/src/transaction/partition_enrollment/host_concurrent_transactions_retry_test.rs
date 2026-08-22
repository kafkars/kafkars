//! Bounded same-coordinator retries for `CONCURRENT_TRANSACTIONS` enrollment terminals.

use std::sync::atomic::Ordering;

use kafka_client_core::{Deadline, DeliveryStatus, Moment, ProducerRetryPolicy, TransactionEpoch};

use super::{
    TransactionPartitionEnrollmentAdmission, TransactionPartitionEnrollmentFailureKind,
    TransactionPartitionEnrollmentOwner, TransactionPartitionEnrollmentTerminal,
    TransactionPartitionEnrollmentTurn,
    host_support_test::{FakePort, batch, deadline, epochs, owner_with_retry_policy},
    port::TransactionPartitionEnrollmentPortFact,
};

#[test]
fn concurrent_transactions_retries_same_request_after_bounded_backoff() {
    let (epoch, _) = epochs();
    let policy = ProducerRetryPolicy::try_fixed(1, 5)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let mut owner = owner_with_retry_policy(epoch, policy);
    assert!(matches!(
        owner
            .try_enroll(epoch, batch("orders", 2), deadline(20))
            .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind())),
        TransactionPartitionEnrollmentAdmission::Pending
    ));
    let mut first = FakePort::accepted(epoch, concurrent_transactions());

    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(first.discarded.load(Ordering::Acquire));
    assert!(owner.take_terminal().is_none());
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(7)));

    let mut replacement =
        FakePort::accepted(epoch, TransactionPartitionEnrollmentPortFact::Enrolled);
    assert_eq!(
        owner.turn_with(Moment::from_tick(6), &mut replacement),
        TransactionPartitionEnrollmentTurn::Idle
    );
    assert!(replacement.requests.is_empty());
    assert_eq!(
        owner.turn_with(Moment::from_tick(7), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(first.requests.len(), 1);
    assert_eq!(replacement.requests.len(), 1);
    assert_eq!(replacement.requests[0], first.requests[0]);
    assert_eq!(
        owner.turn_with(Moment::from_tick(8), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    let Some(TransactionPartitionEnrollmentTerminal::Enrolled(fence)) = owner.take_terminal()
    else {
        panic!("replacement success must enroll the exact batch");
    };
    assert_eq!(fence.epoch(), epoch);
    assert_eq!(fence.into_batch().partition(), 2);
}

#[test]
fn replacement_not_sent_cannot_strengthen_concurrent_transactions_certainty() {
    let policy = ProducerRetryPolicy::try_fixed(1, 1)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let (epoch, mut owner) = pending_owner(policy, 20);
    let _first = drive_to_backoff(&mut owner, epoch);
    let mut replacement = FakePort::accepted(
        epoch,
        TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::Transport,
            delivery: DeliveryStatus::NotSent,
        },
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(3), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(4), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_abort_required(
        &mut owner,
        TransactionPartitionEnrollmentFailureKind::Transport,
    );
}

#[test]
fn replacement_submission_rejection_cannot_strengthen_concurrent_transactions_certainty() {
    let policy = ProducerRetryPolicy::try_fixed(1, 1)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let (epoch, mut owner) = pending_owner(policy, 20);
    let _first = drive_to_backoff(&mut owner, epoch);

    let mut replacement = FakePort::rejected();
    assert_eq!(
        owner.turn_with(Moment::from_tick(3), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_abort_required(
        &mut owner,
        TransactionPartitionEnrollmentFailureKind::DriverRejected,
    );
}

#[test]
fn deadline_during_concurrent_transactions_backoff_preserves_uncertainty() {
    let policy = ProducerRetryPolicy::try_fixed(1, 2)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let (epoch, mut owner) = pending_owner(policy, 5);
    let _first = drive_to_backoff(&mut owner, epoch);
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(4)));

    let mut port = FakePort::rejected();
    assert_eq!(
        owner.turn_with(Moment::from_tick(5), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(port.requests.is_empty());
    assert_abort_required(
        &mut owner,
        TransactionPartitionEnrollmentFailureKind::DeadlineElapsed,
    );
}

#[test]
fn shutdown_during_concurrent_transactions_backoff_preserves_uncertainty() {
    let policy = ProducerRetryPolicy::try_fixed(1, 5)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let (epoch, mut owner) = pending_owner(policy, 20);
    let _first = drive_to_backoff(&mut owner, epoch);

    owner.recover_after_driver_shutdown();
    assert_abort_required(
        &mut owner,
        TransactionPartitionEnrollmentFailureKind::DriverClosed,
    );
}

#[test]
fn concurrent_transactions_retry_exhaustion_preserves_exact_uncertain_failure() {
    let (epoch, _) = epochs();
    let policy = ProducerRetryPolicy::try_fixed(1, 1)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let mut owner = owner_with_retry_policy(epoch, policy);
    let _admission = owner
        .try_enroll(epoch, batch("orders", 2), deadline(20))
        .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind()));
    let mut first = FakePort::accepted(epoch, concurrent_transactions());
    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );

    let mut replacement = FakePort::accepted(epoch, concurrent_transactions());
    assert_eq!(
        owner.turn_with(Moment::from_tick(3), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(4), &mut replacement),
        TransactionPartitionEnrollmentTurn::Progress
    );
    let Some(TransactionPartitionEnrollmentTerminal::AbortRequired {
        kind:
            TransactionPartitionEnrollmentFailureKind::Broker {
                code: 51,
                fenced: false,
            },
        delivery: DeliveryStatus::PossiblySent,
        batch,
    }) = owner.take_terminal()
    else {
        panic!("exhausted concurrent-transaction retry must require abort");
    };
    assert_eq!(batch.partition(), 2);
    assert_eq!(first.requests.len(), 1);
    assert_eq!(replacement.requests.len(), 1);
}

fn pending_owner(
    policy: ProducerRetryPolicy,
    deadline_tick: u64,
) -> (TransactionEpoch, TransactionPartitionEnrollmentOwner) {
    let (epoch, _) = epochs();
    let mut owner = owner_with_retry_policy(epoch, policy);
    let _admission = owner
        .try_enroll(epoch, batch("orders", 2), deadline(deadline_tick))
        .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind()));
    (epoch, owner)
}

fn drive_to_backoff(
    owner: &mut TransactionPartitionEnrollmentOwner,
    epoch: TransactionEpoch,
) -> FakePort {
    let mut first = FakePort::accepted(epoch, concurrent_transactions());
    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(first.discarded.load(Ordering::Acquire));
    assert!(owner.take_terminal().is_none());
    first
}

fn assert_abort_required(
    owner: &mut TransactionPartitionEnrollmentOwner,
    expected: TransactionPartitionEnrollmentFailureKind,
) {
    let Some(TransactionPartitionEnrollmentTerminal::AbortRequired {
        kind,
        delivery: DeliveryStatus::PossiblySent,
        batch,
    }) = owner.take_terminal()
    else {
        panic!("uncertain retry terminal must require abort");
    };
    assert_eq!(kind, expected);
    assert_eq!(batch.partition(), 2);
}

const fn concurrent_transactions() -> TransactionPartitionEnrollmentPortFact {
    TransactionPartitionEnrollmentPortFact::RetryableConcurrentTransactions {
        kind: TransactionPartitionEnrollmentFailureKind::Broker {
            code: 51,
            fenced: false,
        },
        delivery: DeliveryStatus::PossiblySent,
    }
}
