//! Exact synchronous-rejection and accepted-call correlation scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasPlan, Moment,
};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, AlterClientQuotasDeliveryStatus, AlterClientQuotasFailureKind,
        AlterClientQuotasOutcome,
    },
    clock::OperationDeadline,
    driver::{AlterClientQuotasCall, DriverOwner},
};

use super::{AlterClientQuotasHost, AlterClientQuotasHostError, AlterClientQuotasTurn};

#[test]
fn synchronous_rejection_preserves_exact_not_sent_settlement() {
    let (mut host, notifier) = host();
    let expected = plan("alice", true);
    let admission = admit(&mut host, expected.clone());
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, retained_limit) = submission.into_parts();
    assert_eq!(plan, expected);

    host.reject_handoff(operation_id, plan, retained_limit)
        .unwrap_or_else(|error| panic!("reject exact submission: {error}"));
    let AlterClientQuotasOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(failure.kind(), AlterClientQuotasFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), AlterClientQuotasDeliveryStatus::NotSent);

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_validate_only_retains_rejection_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan("alice", false));
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _plan, retained_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(operation_id, plan("alice", true), retained_limit),
        Err(AlterClientQuotasHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_retained_limit_retains_rejection_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan("alice", false));
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, retained_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(operation_id, plan, retained_limit - 1),
        Err(AlterClientQuotasHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_survives_recovery_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan("alice", false));
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, _expected, retained_limit) = submission.into_parts();
    let mismatch = plan("bob", false);
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterClientQuotasCall::submit(
        &driver,
        mismatch.clone(),
        retained_limit,
        deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched call"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(AlterClientQuotasHostError::SubmissionMismatch)
    ));
    assert!(host.call_matches_for_test(&mismatch, retained_limit));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterClientQuotasHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_ownership_matches_for_test(&mismatch, retained_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

fn plan(name: &str, validate_only: bool) -> AlterClientQuotasPlan {
    AlterClientQuotasPlan::new(
        vec![AlterClientQuotaEntry::new(
            AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                "user".to_owned(),
                Some(name.to_owned()),
            )]),
            vec![AlterClientQuotaOperation::set(
                "producer_byte_rate".to_owned(),
                4096.0,
            )],
        )],
        validate_only,
    )
    .unwrap_or_else(|error| panic!("valid alteration plan: {error}"))
}

fn admit(
    host: &mut AlterClientQuotasHost,
    plan: AlterClientQuotasPlan,
) -> super::AlterClientQuotasAdmission {
    host.try_admit(Moment::from_tick(1), deadline(), plan)
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"))
}

fn host() -> (AlterClientQuotasHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    (
        AlterClientQuotasHost::new(ports.alter_client_quotas),
        notifier,
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
