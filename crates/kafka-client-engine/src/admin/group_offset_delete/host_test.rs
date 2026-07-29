//! Combined-envelope, deadline, recovery, and reclamation scenarios.

use std::time::Instant;

use kafka_client_core::{
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsMachineError,
    DeleteConsumerGroupOffsetsPlan, Moment,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, GroupOffsetDeleteCall},
};

use super::{
    DeleteConsumerGroupOffsetsAdmissionErrorKind, DeleteConsumerGroupOffsetsDeliveryStatus,
    DeleteConsumerGroupOffsetsFailureKind, DeleteConsumerGroupOffsetsHostError,
    DeleteConsumerGroupOffsetsOutcome, DeleteConsumerGroupOffsetsTurn,
    host::DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
};

#[test]
fn admission_atomically_reserves_the_complete_four_mib_envelope() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(DeleteConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)
    ));
    let DeleteConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take deletion submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, scratch_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert!(scratch_limit > DELETE_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES / 2);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn untouched_recovery_is_definitely_unsent() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover deletion host: {error}"));
    let DeleteConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupOffsetsFailureKind::DriverRejected,
            DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    let _progress = host
        .turn(Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim deletion terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));
    let DeleteConsumerGroupOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take deletion submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(DeleteConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(DeleteConsumerGroupOffsetsHostError::Machine(
            DeleteConsumerGroupOffsetsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset deletion: {error:?}"));
    let DeleteConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take deletion submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, scratch_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = GroupOffsetDeleteCall::submit(
        &driver,
        submitted_plan,
        scratch_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(DeleteConsumerGroupOffsetsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let DeleteConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupOffsetsFailureKind::Transport,
            DeleteConsumerGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::delete_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed deletion: {error:?}"));
    let DeleteConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            DeleteConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(DeleteConsumerGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

pub(super) fn plan() -> DeleteConsumerGroupOffsetsPlan {
    DeleteConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            DeleteConsumerGroupOffsetTarget::new("orders".to_owned(), 2),
            DeleteConsumerGroupOffsetTarget::new("audit".to_owned(), 0),
        ],
    )
    .unwrap_or_else(|error| panic!("valid offset-deletion plan: {error}"))
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
