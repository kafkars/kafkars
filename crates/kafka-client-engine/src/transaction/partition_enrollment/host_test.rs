//! Exact successful enrollment and epoch-release ownership scenarios.

use std::sync::atomic::Ordering;

use kafka_client_core::Moment;

use super::{
    TransactionPartitionEnrollmentAdmission, TransactionPartitionEnrollmentEpochError,
    TransactionPartitionEnrollmentTerminal, TransactionPartitionEnrollmentTurn,
    host_support_test::{FakePort, RecordedRequest, batch, deadline, epochs, owner, settle},
    port::TransactionPartitionEnrollmentPortFact,
};

#[test]
fn success_retains_exact_request_deadline_epoch_and_batch_fence() {
    let (epoch, _) = epochs();
    let mut owner = owner(epoch);
    let deadline = deadline(20);
    assert!(matches!(
        owner.try_enroll(epoch, batch("orders", 2), deadline),
        Ok(TransactionPartitionEnrollmentAdmission::Pending)
    ));
    let mut port = FakePort::accepted(epoch, TransactionPartitionEnrollmentPortFact::Enrolled);

    assert_eq!(
        owner.turn_with(Moment::from_tick(1), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert_eq!(
        port.requests,
        [RecordedRequest {
            epoch,
            transactional_id: "writer".to_owned(),
            producer_id: 41,
            producer_epoch: 3,
            topic: "orders".to_owned(),
            partition: 2,
            deadline: deadline.transport(),
        }]
    );
    assert!(!port.discarded.load(Ordering::Acquire));
    assert_eq!(
        owner.turn_with(Moment::from_tick(2), &mut port),
        TransactionPartitionEnrollmentTurn::Progress
    );
    assert!(port.discarded.load(Ordering::Acquire));
    let Some(TransactionPartitionEnrollmentTerminal::Enrolled(fence)) = owner.take_terminal()
    else {
        panic!("successful enrollment must produce one exact fence");
    };
    assert_eq!(fence.epoch(), epoch);
    assert_eq!(fence.into_batch().partition(), 2);
    assert_eq!(owner.enrolled_partitions(), 1);
    assert_eq!(owner.retained_topic_bytes(), "orders".len());

    let TransactionPartitionEnrollmentAdmission::Enrolled(cached) = owner
        .try_enroll(epoch, batch("orders", 2), deadline)
        .unwrap_or_else(|failure| panic!("cached target rejected: {:?}", failure.kind()))
    else {
        panic!("same-epoch enrolled target must not resubmit");
    };
    assert_eq!(cached.epoch(), epoch);
}

#[test]
fn end_txn_release_clears_only_the_exact_epoch_enrollment_set() {
    let (first, second) = epochs();
    let mut owner = owner(first);
    settle(
        &mut owner,
        first,
        TransactionPartitionEnrollmentPortFact::Enrolled,
    );
    let Some(TransactionPartitionEnrollmentTerminal::Enrolled(fence)) = owner.take_terminal()
    else {
        panic!("enrollment fence expected");
    };
    assert_eq!(fence.epoch(), first);
    drop(fence.into_batch());
    assert_eq!(
        owner.release_epoch(second),
        Err(TransactionPartitionEnrollmentEpochError::EpochMismatch)
    );
    assert_eq!(owner.enrolled_partitions(), 1);
    owner
        .release_epoch(first)
        .unwrap_or_else(|error| panic!("release exact epoch: {error:?}"));
    assert_eq!(owner.enrolled_partitions(), 0);
    assert_eq!(owner.retained_topic_bytes(), 0);
    owner
        .activate_epoch(second)
        .unwrap_or_else(|error| panic!("activate next epoch: {error:?}"));
    assert!(matches!(
        owner.try_enroll(second, batch("orders", 2), deadline(30)),
        Ok(TransactionPartitionEnrollmentAdmission::Pending)
    ));
    owner.recover_after_driver_shutdown();
}
