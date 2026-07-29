//! Non-secret rejection and accepted-call correlation scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ALTER_USER_SCRAM_CREDENTIALS_SHA_256, AlterUserScramCredentialChange,
    AlterUserScramCredentialsPlan, Moment,
};

use crate::{
    EngineConfig,
    admin::{
        AdminCompletionNotifier, AlterUserScramCredentialsDeliveryStatus,
        AlterUserScramCredentialsFailureKind, AlterUserScramCredentialsOutcome,
    },
    clock::OperationDeadline,
    driver::{AlterUserScramCredentialsCall, DriverOwner},
    protocol::admin::alter_user_scram_credentials::{
        AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestRef,
        PreparedAlterUserScramCredentialsRequest, alter_user_scram_credentials_request,
    },
};

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES, AlterUserScramCredentialsHost,
    AlterUserScramCredentialsHostError, AlterUserScramCredentialsTurn,
};

#[test]
fn synchronous_rejection_preserves_exact_not_sent_settlement() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, "alice");
    let (operation_id, _deadline, plan, prepared, request_bytes, result_limit) =
        take_submission(&mut host);
    drop(prepared);

    host.reject_handoff(operation_id, plan, request_bytes, result_limit)
        .unwrap_or_else(|error| panic!("reject exact submission: {error}"));
    let AlterUserScramCredentialsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(
        failure.kind(),
        AlterUserScramCredentialsFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        AlterUserScramCredentialsDeliveryStatus::NotSent
    );

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_prepared_request_charge_retains_rejection_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, "alice");
    let (operation_id, _deadline, plan, prepared, request_bytes, result_limit) =
        take_submission(&mut host);
    drop(prepared);

    assert!(matches!(
        host.reject_handoff(operation_id, plan, request_bytes + 1, result_limit),
        Err(AlterUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_result_limit_retains_rejection_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, "alice");
    let (operation_id, _deadline, plan, prepared, request_bytes, result_limit) =
        take_submission(&mut host);
    drop(prepared);

    assert!(matches!(
        host.reject_handoff(operation_id, plan, request_bytes, result_limit - 1),
        Err(AlterUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_submission_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_call_survives_recovery_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, "alice");
    let (operation_id, deadline, _plan, prepared, _request_bytes, result_limit) =
        take_submission(&mut host);
    drop(prepared);
    let mismatch = plan("carol");
    let mismatched_prepared = prepared_delete("carol");
    let mismatch_bytes = mismatched_prepared.retained_heap_bytes();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterUserScramCredentialsCall::submit(
        &driver,
        mismatch.clone(),
        mismatched_prepared,
        result_limit,
        deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted mismatched call"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(AlterUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.call_matches_for_test(&mismatch, mismatch_bytes, result_limit));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_ownership_matches_for_test(&mismatch, mismatch_bytes, result_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

fn admit(
    host: &mut AlterUserScramCredentialsHost,
    user: &str,
) -> super::AlterUserScramCredentialsAdmission {
    host.try_admit(
        Moment::from_tick(1),
        deadline(),
        plan(user),
        prepared_delete(user),
    )
    .unwrap_or_else(|error| panic!("admit alteration: {error:?}"))
}

fn take_submission(
    host: &mut AlterUserScramCredentialsHost,
) -> (
    kafka_client_core::OperationId,
    OperationDeadline,
    AlterUserScramCredentialsPlan,
    PreparedAlterUserScramCredentialsRequest,
    usize,
    usize,
) {
    let AlterUserScramCredentialsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    submission.into_parts()
}

fn plan(user: &str) -> AlterUserScramCredentialsPlan {
    AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
        user.to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    )])
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn prepared_delete(user: &str) -> PreparedAlterUserScramCredentialsRequest {
    let alterations = [AlterUserScramCredentialAlterationRef::delete(
        user,
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    )];
    alter_user_scram_credentials_request(
        AlterUserScramCredentialsRequestRef::new(&alterations),
        ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
    )
    .unwrap_or_else(|error| panic!("prepare deletion: {error:?}"))
}

fn host() -> (AlterUserScramCredentialsHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    (
        AlterUserScramCredentialsHost::new(ports.alter_user_scram_credentials),
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
