//! Combined-envelope, deadline, recovery, and reclamation scenarios.

use std::time::Instant;

use kafka_client_core::{AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsPlan, Moment};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, GroupOffsetAlterCall},
};

use super::{
    AlterConsumerGroupOffsetsAdmissionErrorKind, AlterConsumerGroupOffsetsDeliveryStatus,
    AlterConsumerGroupOffsetsFailureKind, AlterConsumerGroupOffsetsOutcome,
    AlterConsumerGroupOffsetsTurn, host::ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES,
};

#[test]
fn admission_atomically_reserves_the_complete_four_mib_envelope() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admit offset alteration: {error:?}"));

    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(
        host.retained_bytes_for_test(),
        ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline, plan()),
        Err(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)
    ));
    let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take alteration submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, scratch_limit, result_limit) =
        submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, plan());
    assert!(scratch_limit > 0);
    assert!(scratch_limit < ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES);
    assert!(result_limit > 0);
    assert!(result_limit < ALTER_CONSUMER_GROUP_OFFSETS_RETAINED_BYTES);
    assert_eq!(host.next_deadline(), None);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn metadata_heavy_generated_request_peak_is_rejected_before_reservation() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let targets = (0..33)
        .map(|partition| {
            AlterConsumerGroupOffsetTarget::new(
                "orders".to_owned(),
                partition,
                91,
                None,
                Some("x".repeat(i16::MAX as usize)),
            )
        })
        .collect();
    let plan = AlterConsumerGroupOffsetsPlan::new("payments".to_owned(), targets)
        .unwrap_or_else(|error| panic!("valid large-metadata plan: {error}"));

    assert!(matches!(
        host.try_admit(Moment::from_tick(1), deadline(10), plan),
        Err(AlterConsumerGroupOffsetsAdmissionErrorKind::RetainedBytes)
    ));
    assert_eq!(host.unsettled(), 0);
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn untouched_and_handed_off_recovery_preserve_delivery_boundary() {
    for handed_off in [false, true] {
        let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
        let admission = host
            .try_admit(Moment::from_tick(1), deadline(10), plan())
            .unwrap_or_else(|error| panic!("admit offset alteration: {error:?}"));
        if handed_off {
            let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
                .turn(Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("take alteration submission: {error}"))
            else {
                panic!("submission expected");
            };
            let (_id, _deadline, plan, request_scratch_limit, result_limit) =
                submission.into_parts();
            host.retain_recovered_call_for_test(plan, request_scratch_limit, result_limit);
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover alteration host: {error}"));
        let AlterConsumerGroupOffsetsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("recovery failure expected");
        };
        let expected = if handed_off {
            (
                AlterConsumerGroupOffsetsFailureKind::Transport,
                AlterConsumerGroupOffsetsDeliveryStatus::PossiblySent,
            )
        } else {
            (
                AlterConsumerGroupOffsetsFailureKind::DriverRejected,
                AlterConsumerGroupOffsetsDeliveryStatus::NotSent,
            )
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);
        let _progress = host
            .turn(Moment::from_tick(3))
            .unwrap_or_else(|error| panic!("reclaim alteration terminal: {error}"));
        assert_eq!(host.retained_bytes_for_test(), 0);

        drop(host);
        crate::admin::test_support::stop_notifier(notifier);
    }
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset alteration: {error:?}"));
    let AlterConsumerGroupOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take alteration submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(super::AlterConsumerGroupOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit offset alteration: {error:?}"));
    let AlterConsumerGroupOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take alteration submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, scratch_limit, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = GroupOffsetAlterCall::submit(
        &driver,
        submitted_plan,
        scratch_limit,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(super::AlterConsumerGroupOffsetsHostError::CallCompletion)
    ));
    assert_eq!(
        host.unsettled(),
        1,
        "completion failure must retain accepted call evidence"
    );
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AlterConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AlterConsumerGroupOffsetsFailureKind::Transport,
            AlterConsumerGroupOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn expired_public_deadline_never_reaches_driver_handoff() {
    let (mut host, notifier) = crate::admin::test_support::alter_consumer_group_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(10), deadline(10), plan())
        .unwrap_or_else(|error| panic!("admit elapsed alteration: {error:?}"));
    let AlterConsumerGroupOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe elapsed terminal: {error}"))
    else {
        panic!("deadline failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed,
            AlterConsumerGroupOffsetsDeliveryStatus::NotSent,
        )
    );
    assert!(matches!(
        host.turn(Moment::from_tick(11)),
        Ok(AlterConsumerGroupOffsetsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

pub(super) fn plan() -> AlterConsumerGroupOffsetsPlan {
    AlterConsumerGroupOffsetsPlan::new(
        "payments".to_owned(),
        vec![
            AlterConsumerGroupOffsetTarget::new(
                "orders".to_owned(),
                2,
                91,
                Some(7),
                Some("checkpoint-a".to_owned()),
            ),
            AlterConsumerGroupOffsetTarget::new("audit".to_owned(), 0, 42, None, None),
        ],
    )
    .unwrap_or_else(|error| panic!("valid offset-alteration plan: {error}"))
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
