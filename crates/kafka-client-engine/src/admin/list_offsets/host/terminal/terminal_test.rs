//! Accepted-call completion and shutdown-recovery ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AdminListOffsetSpec, AdminListOffsetTarget, AdminListOffsetsMachineError, AdminListOffsetsPlan,
    Moment, ReadIsolation,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{AdminListOffsetsCall, DriverOwner, RecoveredAdminListOffsetsCall},
};

use super::super::{AdminListOffsetsHostError, AdminListOffsetsTurn};
use crate::admin::list_offsets::{
    AdminListOffsetsDeliveryStatus, AdminListOffsetsFailureKind, AdminListOffsetsOutcome,
};

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit Admin ListOffsets: {error:?}"));
    let AdminListOffsetsTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AdminListOffsetsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit Admin ListOffsets: {error:?}"));
    host.operations[0].recovered_call = Some(RecoveredAdminListOffsetsCall::for_test(
        target(),
        ReadIsolation::ReadUncommitted,
    ));

    assert!(matches!(
        host.settle_recovered_transport(0),
        Err(AdminListOffsetsHostError::Machine(
            AdminListOffsetsMachineError::InvalidState
        ))
    ));
    assert!(host.operations[0].recovered_call.is_some());

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn mismatched_recovered_correlation_blocks_core_settlement_and_publication() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit Admin ListOffsets: {error:?}"));
    host.operations[0].recovered_call = Some(RecoveredAdminListOffsetsCall::for_test(
        AdminListOffsetTarget::new("other".to_owned(), 9, AdminListOffsetSpec::Earliest),
        ReadIsolation::ReadCommitted,
    ));

    assert!(matches!(
        host.settle_recovered_transport(0),
        Err(AdminListOffsetsHostError::SubmissionMismatch)
    ));
    assert!(host.operations[0].recovered_call.is_some());
    assert_eq!(host.unsettled(), 1);

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = crate::admin::test_support::admin_list_offsets_host();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan())
        .unwrap_or_else(|error| panic!("admit Admin ListOffsets: {error:?}"));
    let AdminListOffsetsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, target, read_isolation) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AdminListOffsetsCall::submit(
        &driver,
        target,
        read_isolation,
        1_000,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(AdminListOffsetsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AdminListOffsetsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            AdminListOffsetsFailureKind::Transport,
            AdminListOffsetsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

fn plan() -> AdminListOffsetsPlan {
    AdminListOffsetsPlan::new(vec![target()])
        .unwrap_or_else(|error| panic!("valid Admin ListOffsets plan: {error}"))
}

fn target() -> AdminListOffsetTarget {
    AdminListOffsetTarget::new("orders".to_owned(), 2, AdminListOffsetSpec::Latest)
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(20),
        Instant::now() + Duration::from_secs(1),
    )
}
