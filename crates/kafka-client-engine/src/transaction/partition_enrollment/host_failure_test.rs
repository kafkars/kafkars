//! Exact local rejection, delivery uncertainty, and fencing scenarios.

use kafka_client_core::{DeliveryStatus, Moment, TransactionalProducerIdentity};

use super::{
    TransactionPartitionEnrollmentFailureKind, TransactionPartitionEnrollmentTerminal,
    TransactionPartitionEnrollmentTurn,
    host_support_test::{FakePort, batch, batch_with_identity, deadline, epochs, owner, terminal},
    port::TransactionPartitionEnrollmentPortFact,
};

#[test]
fn definitely_unsent_rejections_return_exact_batch_without_abort_required() {
    let (epoch, stale) = epochs();
    let mut owner = owner(epoch);
    let stale_failure = owner
        .try_enroll(stale, batch("orders", 1), deadline(10))
        .err()
        .unwrap_or_else(|| panic!("stale epoch must reject"));
    assert_eq!(
        stale_failure.kind(),
        TransactionPartitionEnrollmentFailureKind::EpochMismatch
    );
    assert_eq!(stale_failure.into_batch().partition(), 1);
    let wrong = TransactionalProducerIdentity::try_new(99, 1)
        .unwrap_or_else(|| panic!("test identity must be valid"));
    let failure = owner
        .try_enroll(epoch, batch_with_identity("orders", 2, wrong), deadline(10))
        .err()
        .unwrap_or_else(|| panic!("owner mismatch must reject"));
    assert_eq!(
        failure.kind(),
        TransactionPartitionEnrollmentFailureKind::OwnerMismatch
    );
    assert_eq!(failure.into_batch().partition(), 2);

    let _admission = owner
        .try_enroll(epoch, batch("orders", 3), deadline(10))
        .unwrap_or_else(|failure| panic!("valid admission: {:?}", failure.kind()));
    let mut port = FakePort::accepted(epoch, TransactionPartitionEnrollmentPortFact::Enrolled);
    assert_eq!(
        owner.turn_with(Moment::from_tick(10), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(port.requests.is_empty());
    let Some(TransactionPartitionEnrollmentTerminal::Rejected(failure)) = owner.take_terminal()
    else {
        panic!("elapsed-before-submit must remain local rejection");
    };
    assert_eq!(
        failure.kind(),
        TransactionPartitionEnrollmentFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.into_batch().partition(), 3);

    let _admission = owner
        .try_enroll(epoch, batch("audit", 1), deadline(20))
        .unwrap_or_else(|failure| panic!("valid admission: {:?}", failure.kind()));
    let mut rejecting = FakePort::rejected();
    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut rejecting),
        TransactionPartitionEnrollmentTurn::Progress
    );
    let Some(TransactionPartitionEnrollmentTerminal::Rejected(failure)) = owner.take_terminal()
    else {
        panic!("driver submission rejection must return exact batch");
    };
    assert_eq!(
        failure.kind(),
        TransactionPartitionEnrollmentFailureKind::DriverRejected
    );
    assert_eq!(failure.into_batch().topic().as_ref(), "audit");
}

#[test]
fn accepted_delivery_uncertainty_aborts_and_exact_fencing_is_fatal() {
    let (epoch, _) = epochs();
    let rejected = terminal(
        epoch,
        TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::Transport,
            delivery: DeliveryStatus::NotSent,
        },
    );
    assert!(matches!(
        rejected,
        TransactionPartitionEnrollmentTerminal::Rejected(_)
    ));

    let abort = terminal(
        epoch,
        TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        },
    );
    assert!(matches!(
        abort,
        TransactionPartitionEnrollmentTerminal::AbortRequired {
            kind: TransactionPartitionEnrollmentFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
            ..
        }
    ));

    let fatal = terminal(
        epoch,
        TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::Broker {
                code: 90,
                fenced: true,
            },
            delivery: DeliveryStatus::PossiblySent,
        },
    );
    assert!(matches!(
        fatal,
        TransactionPartitionEnrollmentTerminal::Fatal {
            kind: TransactionPartitionEnrollmentFailureKind::Broker {
                code: 90,
                fenced: true,
            },
            ..
        }
    ));
}
