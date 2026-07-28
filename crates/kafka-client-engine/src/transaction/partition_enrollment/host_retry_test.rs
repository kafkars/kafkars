//! Bounded coordinator-replacement retries under the original enrollment deadline.

use std::sync::atomic::Ordering;

use kafka_client_core::{Deadline, DeliveryStatus, Moment, ProducerRetryPolicy};

use super::{
    TransactionPartitionEnrollmentAdmission, TransactionPartitionEnrollmentFailureKind,
    TransactionPartitionEnrollmentTerminal, TransactionPartitionEnrollmentTurn,
    host_support_test::{FakePort, batch, deadline, epochs, owner_with_retry_policy},
    port::TransactionPartitionEnrollmentPortFact,
};

#[test]
fn refreshed_coordinator_load_retries_once_after_the_configured_backoff() {
    let (epoch, _) = epochs();
    let retry_policy = ProducerRetryPolicy::try_fixed(1, 5)
        .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
    let mut owner = owner_with_retry_policy(epoch, retry_policy);
    assert!(matches!(
        owner
            .try_enroll(epoch, batch("orders", 2), deadline(20))
            .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind())),
        TransactionPartitionEnrollmentAdmission::Pending
    ));
    let retryable = TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss {
        kind: TransactionPartitionEnrollmentFailureKind::Broker {
            code: 14,
            fenced: false,
        },
        delivery: DeliveryStatus::PossiblySent,
    };
    let mut first = FakePort::accepted(epoch, retryable);

    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut first),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(owner.take_terminal().is_none());
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(7)));
    let first_request = first
        .requests
        .first()
        .unwrap_or_else(|| panic!("first attempt must be recorded"));

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
    let replacement_request = replacement
        .requests
        .first()
        .unwrap_or_else(|| panic!("replacement attempt must be recorded"));
    assert_eq!(replacement_request.epoch, first_request.epoch);
    assert_eq!(
        replacement_request.transactional_id,
        first_request.transactional_id
    );
    assert_eq!(replacement_request.producer_id, first_request.producer_id);
    assert_eq!(
        replacement_request.producer_epoch,
        first_request.producer_epoch
    );
    assert_eq!(replacement_request.topic, first_request.topic);
    assert_eq!(replacement_request.partition, first_request.partition);
    assert_eq!(replacement_request.deadline, first_request.deadline);

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
fn coordinator_load_without_retry_budget_preserves_possibly_sent_certainty() {
    let (epoch, _) = epochs();
    let mut owner = owner_with_retry_policy(epoch, ProducerRetryPolicy::none());
    let _admission = owner
        .try_enroll(epoch, batch("orders", 2), deadline(20))
        .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind()));
    let mut port = FakePort::accepted(
        epoch,
        TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss {
            kind: TransactionPartitionEnrollmentFailureKind::Broker {
                code: 14,
                fenced: false,
            },
            delivery: DeliveryStatus::PossiblySent,
        },
    );

    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    let Some(TransactionPartitionEnrollmentTerminal::AbortRequired {
        kind: TransactionPartitionEnrollmentFailureKind::Broker { code: 14, .. },
        delivery: DeliveryStatus::PossiblySent,
        batch,
    }) = owner.take_terminal()
    else {
        panic!("coordinator load without budget must preserve uncertain delivery");
    };
    assert_eq!(batch.partition(), 2);
    assert_eq!(port.requests.len(), 1);
}

#[test]
fn disabled_or_deadline_exhausted_retry_policy_exposes_the_original_not_sent_failure() {
    let (epoch, _) = epochs();
    for retry_policy in [
        ProducerRetryPolicy::none(),
        ProducerRetryPolicy::try_fixed(1, 20)
            .unwrap_or_else(|error| panic!("valid retry policy: {error}")),
    ] {
        let mut owner = owner_with_retry_policy(epoch, retry_policy);
        let _admission = owner
            .try_enroll(epoch, batch("orders", 2), deadline(20))
            .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind()));
        let mut port = FakePort::accepted(
            epoch,
            TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss {
                kind: TransactionPartitionEnrollmentFailureKind::Transport,
                delivery: DeliveryStatus::NotSent,
            },
        );
        assert_eq!(
            owner.turn_with(Moment::from_tick(1), &mut port),
            TransactionPartitionEnrollmentTurn::Progress
        );
        assert_eq!(
            owner.turn_with(Moment::from_tick(2), &mut port),
            TransactionPartitionEnrollmentTurn::Progress
        );
        let Some(TransactionPartitionEnrollmentTerminal::Rejected(failure)) = owner.take_terminal()
        else {
            panic!("unavailable replacement must expose the original failure");
        };
        assert_eq!(
            failure.kind(),
            TransactionPartitionEnrollmentFailureKind::Transport
        );
        assert_eq!(failure.into_batch().partition(), 2);
    }
}

#[test]
fn stalled_refresh_expires_with_original_delivery_and_no_replacement_submission() {
    let (epoch, _) = epochs();
    for delivery in [DeliveryStatus::NotSent, DeliveryStatus::PossiblySent] {
        let retry_policy = ProducerRetryPolicy::try_fixed(2, 1)
            .unwrap_or_else(|error| panic!("valid retry policy: {error}"));
        let mut owner = owner_with_retry_policy(epoch, retry_policy);
        let _admission = owner
            .try_enroll(epoch, batch("orders", 2), deadline(5))
            .unwrap_or_else(|failure| panic!("valid enrollment: {:?}", failure.kind()));
        let mut port = FakePort::refresh_stalled(
            epoch,
            TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss {
                kind: TransactionPartitionEnrollmentFailureKind::Broker {
                    code: 16,
                    fenced: false,
                },
                delivery,
            },
        );

        assert_eq!(
            owner.turn_with(Moment::from_tick(1), &mut port),
            TransactionPartitionEnrollmentTurn::Progress
        );
        assert_eq!(
            owner.turn_with(Moment::from_tick(2), &mut port),
            TransactionPartitionEnrollmentTurn::Idle
        );
        assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(5)));
        assert_eq!(
            owner.turn_with(Moment::from_tick(5), &mut port),
            TransactionPartitionEnrollmentTurn::Progress
        );
        assert_eq!(port.requests.len(), 1);
        assert!(port.discarded.load(Ordering::Acquire));

        match (delivery, owner.take_terminal()) {
            (
                DeliveryStatus::NotSent,
                Some(TransactionPartitionEnrollmentTerminal::Rejected(failure)),
            ) => {
                assert_eq!(
                    failure.kind(),
                    TransactionPartitionEnrollmentFailureKind::DeadlineElapsed
                );
            }
            (
                DeliveryStatus::PossiblySent,
                Some(TransactionPartitionEnrollmentTerminal::AbortRequired {
                    kind: TransactionPartitionEnrollmentFailureKind::DeadlineElapsed,
                    delivery: DeliveryStatus::PossiblySent,
                    ..
                }),
            ) => {}
            _ => panic!("deadline must retain driver-authoritative delivery"),
        }
    }
}
