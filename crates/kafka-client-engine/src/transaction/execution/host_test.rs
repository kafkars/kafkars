//! Installed send ownership, lifecycle gating, and recovery tests.

use kafka_client_core::{
    CompressionPolicy, DeliveryStatus, TransactionEndMode, TransactionLifecycleMachineError,
    TransactionalOwnerId,
};

use crate::transaction::{
    TransactionExecutionSendAdmissionErrorKind, TransactionLifecycleHostError,
    send::{
        TransactionSendAdmissionFailureKind, TransactionSendFailureKind, TransactionSendTerminal,
    },
};

use super::test_support::{FakeProducePort, Fixture, deadline, drive_send, request};

#[test]
fn send_seam_rejects_a_foreign_owner_without_losing_request() {
    let mut fixture = Fixture::new(CompressionPolicy::None);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let original_deadline = deadline(40);
    let foreign = TransactionalOwnerId::from_raw(fixture.owner_id.get() + 1);
    let Err(failure) = fixture
        .host
        .try_send(foreign, request(epoch, "orders", original_deadline, 1_024))
    else {
        panic!("foreign owner was unexpectedly accepted");
    };

    assert_eq!(
        failure.kind(),
        TransactionExecutionSendAdmissionErrorKind::StaleOwner
    );
    assert_eq!(
        failure.into_input(),
        request(epoch, "orders", original_deadline, 1_024)
    );
    fixture.shutdown_driver();
}

#[test]
fn accepted_send_blocks_commit_until_success_reopens_it() {
    let mut fixture = Fixture::new(CompressionPolicy::Snappy);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let original_deadline = deadline(50);
    let accepted = fixture
        .host
        .try_send(
            fixture.owner_id,
            request(epoch, "orders", original_deadline, 1_024),
        )
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    let send_id = accepted.send_id();
    let observer = accepted.into_observer();
    assert_eq!(
        fixture.host.topic_id_for_test("orders"),
        Some(kafka_client_core::TopicId::from_raw(1))
    );
    let rejected_deadline = deadline(49);
    let Err(rejected) = fixture.host.try_send(
        fixture.owner_id,
        request(epoch, "payments", rejected_deadline, 1_024),
    ) else {
        panic!("second send unexpectedly acquired the occupied fixed slot");
    };
    assert_eq!(
        rejected.kind(),
        TransactionExecutionSendAdmissionErrorKind::Send(TransactionSendAdmissionFailureKind::Busy)
    );
    assert_eq!(
        rejected.into_input(),
        request(epoch, "payments", rejected_deadline, 1_024)
    );
    assert_eq!(
        fixture.host.topic_id_for_test("payments"),
        None,
        "rejected new topic does not commit its staged identity"
    );

    assert_eq!(fixture.host.next_deadline(), Some(original_deadline.core()));
    assert!(matches!(
        fixture
            .host
            .end(epoch, TransactionEndMode::Commit, deadline(51)),
        Err(TransactionLifecycleHostError::Core(
            TransactionLifecycleMachineError::OutstandingSends { count: 1 }
        ))
    ));

    fixture.host.settle_pending_enrolled_for_test();
    let mut port = FakeProducePort::succeeding(epoch, send_id);
    drive_send(&mut fixture, &mut port, 6);
    assert_eq!(port.observed_deadline, Some(original_deadline));
    assert!(port.was_discarded());
    assert!(matches!(
        observer.wait(),
        Ok(TransactionSendTerminal::Succeeded {
            epoch: terminal_epoch,
            send_id: terminal_send_id,
            ..
        }) if terminal_epoch == epoch && terminal_send_id == send_id
    ));
    assert!(
        fixture
            .host
            .end(epoch, TransactionEndMode::Commit, deadline(52))
            .is_ok()
    );
    fixture.shutdown_driver();
}

#[test]
fn local_not_sent_failure_keeps_the_transaction_healthy() {
    let mut fixture = Fixture::new(CompressionPolicy::None);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let accepted = fixture
        .host
        .try_send(fixture.owner_id, request(epoch, "", deadline(60), 1_024))
        .unwrap_or_else(|error| {
            panic!("lifecycle accepts before local enrollment rejection: {error:?}")
        });
    let send_id = accepted.send_id();
    let mut port = FakeProducePort::succeeding(epoch, send_id);
    drive_send(&mut fixture, &mut port, 1);

    let Ok(TransactionSendTerminal::FailedHealthy { failure, .. }) =
        accepted.into_observer().wait()
    else {
        panic!("healthy terminal");
    };
    assert_eq!(
        failure.kind(),
        TransactionSendFailureKind::Enrollment(
            crate::transaction::partition_enrollment::TransactionPartitionEnrollmentFailureKind::InvalidTarget
        )
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    assert!(
        fixture
            .host
            .end(epoch, TransactionEndMode::Commit, deadline(61))
            .is_ok()
    );
    fixture.shutdown_driver();
}

#[test]
fn retained_record_over_budget_returns_exact_input_before_catalog_mutation() {
    let mut fixture = Fixture::new(CompressionPolicy::None);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let original_deadline = deadline(65);
    let Err(failure) = fixture.host.try_send(
        fixture.owner_id,
        request(epoch, "oversized", original_deadline, 1_025),
    ) else {
        panic!("over-budget source record was unexpectedly accepted");
    };

    assert_eq!(
        failure.kind(),
        TransactionExecutionSendAdmissionErrorKind::RetainedRecordBytes {
            actual: 1_025,
            limit: 1_024,
        }
    );
    assert_eq!(
        failure.into_input(),
        request(epoch, "oversized", original_deadline, 1_025)
    );
    assert_eq!(fixture.host.topic_id_for_test("oversized"), None);
    fixture.shutdown_driver();
}

#[test]
fn host_supplies_the_validated_wire_batch_limit() {
    let mut fixture = Fixture::with_limits(CompressionPolicy::None, 8, 1_024, 1_024, 1);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let accepted = fixture
        .host
        .try_send(
            fixture.owner_id,
            request(epoch, "orders", deadline(66), 1_024),
        )
        .unwrap_or_else(|error| {
            panic!("source record fits even though encoded batch will not: {error:?}")
        });
    fixture.host.settle_pending_enrolled_for_test();
    let mut port = FakeProducePort::succeeding(epoch, accepted.send_id());
    drive_send(&mut fixture, &mut port, 3);

    let Ok(TransactionSendTerminal::FailedHealthy { failure, .. }) =
        accepted.into_observer().wait()
    else {
        panic!("host wire limit produces a local materialization terminal");
    };
    assert_eq!(failure.kind(), TransactionSendFailureKind::Materialization);
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
    fixture.shutdown_driver();
}

#[test]
fn owner_loss_and_driver_shutdown_recover_the_send_before_lifecycle() {
    let mut fixture = Fixture::new(CompressionPolicy::None);
    let epoch = fixture
        .host
        .begin()
        .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
    let accepted = fixture
        .host
        .try_send(
            fixture.owner_id,
            request(epoch, "orders", deadline(70), 1_024),
        )
        .unwrap_or_else(|error| panic!("send accepted: {error:?}"));
    drop(accepted.into_observer());
    fixture
        .host
        .owner_lost(deadline(71))
        .unwrap_or_else(|error| panic!("owner loss starts exact drain: {error:?}"));

    fixture
        .host
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("send then lifecycle recover: {error:?}"));
    assert!(fixture.host.is_closed());
    assert_eq!(fixture.host.unsettled(), 0);
    fixture.shutdown_driver();
}
