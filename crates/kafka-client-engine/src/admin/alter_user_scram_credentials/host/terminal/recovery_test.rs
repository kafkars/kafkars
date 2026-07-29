//! Raw non-secret correlation mismatch retention before core settlement.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ALTER_USER_SCRAM_CREDENTIALS_SHA_256, AlterUserScramCredentialChange,
    AlterUserScramCredentialsInput, AlterUserScramCredentialsMachineError,
    AlterUserScramCredentialsPlan, Moment,
};

use crate::{
    admin::AdminCompletionNotifier,
    clock::OperationDeadline,
    protocol::admin::alter_user_scram_credentials::{
        AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestRef,
        alter_user_scram_credentials_request,
    },
};

use super::super::super::{
    AlterUserScramCredentialsHost, AlterUserScramCredentialsHostError,
    AlterUserScramCredentialsTurn,
};
use super::super::ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES;

#[test]
fn recovered_call_and_plan_survive_core_rejection() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut host = AlterUserScramCredentialsHost::new(ports.alter_user_scram_credentials);
    let admission = host
        .try_admit(
            Moment::from_tick(1),
            deadline(),
            plan("alice"),
            prepared_delete("alice"),
        )
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let AlterUserScramCredentialsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, _deadline, expected, prepared, request_bytes, result_limit) =
        submission.into_parts();
    drop(prepared);
    host.retain_recovered_call_for_test(expected.clone(), request_bytes, result_limit);

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AlterUserScramCredentialsHostError::Machine(
            AlterUserScramCredentialsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_matches_for_test(&expected, request_bytes, result_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}

#[test]
fn mismatched_raw_terminal_cannot_settle_core_or_publish() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    let mut host = AlterUserScramCredentialsHost::new(ports.alter_user_scram_credentials);
    let prepared = prepared_delete("alice");
    let admission = host
        .try_admit(Moment::from_tick(1), deadline(), plan("alice"), prepared)
        .unwrap_or_else(|error| panic!("admit alteration: {error:?}"));
    let AlterUserScramCredentialsTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _plan, prepared, request_bytes, result_limit) =
        submission.into_parts();
    drop(prepared);
    host.apply_input_for_test(operation_id, AlterUserScramCredentialsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept synthetic driver ownership: {error}"));
    host.retain_raw_terminal_for_test(plan("carol"), request_bytes, result_limit);

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(AlterUserScramCredentialsHostError::SubmissionMismatch)
    ));
    assert!(host.raw_terminal_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterUserScramCredentialsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}

fn plan(user: &str) -> AlterUserScramCredentialsPlan {
    AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
        user.to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    )])
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn prepared_delete(
    user: &str,
) -> crate::protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest
{
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

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}
