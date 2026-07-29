//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{
    ALTER_USER_SCRAM_CREDENTIALS_SHA_256, AlterUserScramCredentialChange,
    AlterUserScramCredentialsPlan,
};

use crate::{
    admin::{AdminCompletionNotifier, AlterUserScramCredentialsHost},
    clock::MonotonicClock,
    protocol::admin::alter_user_scram_credentials::{
        AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestRef,
        PreparedAlterUserScramCredentialsRequest, alter_user_scram_credentials_request,
    },
};

use super::{
    AlterUserScramCredentialsAdmissionErrorKind, AlterUserScramCredentialsDeliveryStatus,
    AlterUserScramCredentialsFailureKind, AlterUserScramCredentialsOutcome,
    AlterUserScramCredentialsTurn, host::ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_prepared_request_and_full_envelope() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterUserScramCredentialsHost::new(ports.alter_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan("alice"),
            prepared_delete("alice"),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM alteration: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        ALTER_USER_SCRAM_CREDENTIALS_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan("bob"),
            prepared_delete("bob"),
        ),
        Err(AlterUserScramCredentialsAdmissionErrorKind::RetainedBytes)
    ));

    let AlterUserScramCredentialsTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, prepared) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan.affected_users(), ["alice".to_owned()]);
    assert!(format!("{prepared:?}").contains("credential_material"));
    drop(prepared);

    drop(admission.observer);
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterUserScramCredentialsHost::new(ports.alter_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan("alice"),
            prepared_delete("alice"),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM alteration: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched alteration: {error}"));
    let AlterUserScramCredentialsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        AlterUserScramCredentialsFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        AlterUserScramCredentialsDeliveryStatus::NotSent
    );

    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_shutdown_is_conservatively_possibly_sent() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterUserScramCredentialsHost::new(ports.alter_user_scram_credentials);
    let capture = deadline();
    let admission = host
        .try_admit(
            capture.now(),
            capture.operation_deadline(),
            plan("alice"),
            prepared_delete("alice"),
        )
        .unwrap_or_else(|error| panic!("admit SCRAM alteration: {error:?}"));
    let AlterUserScramCredentialsTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handoff: {error}"));
    let AlterUserScramCredentialsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        AlterUserScramCredentialsFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        AlterUserScramCredentialsDeliveryStatus::PossiblySent
    );
    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(user: &str) -> AlterUserScramCredentialsPlan {
    AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
        user.to_owned(),
        ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
    )])
    .unwrap_or_else(|error| panic!("valid deletion plan: {error}"))
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

fn deadline() -> crate::clock::DeadlineCapture {
    Arc::new(MonotonicClock::new())
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("deadline: {error}"))
}

fn stop_notifier(notifier: &mut AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
