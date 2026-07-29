//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotaOperationKind, AlterClientQuotasMachineError,
    AlterClientQuotasPlan,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, AlterClientQuotasHost},
    clock::MonotonicClock,
    driver::{AlterClientQuotasCall, DriverOwner},
};

use super::{
    AlterClientQuotasAdmissionErrorKind, AlterClientQuotasDeliveryStatus,
    AlterClientQuotasFailureKind, AlterClientQuotasHostError, AlterClientQuotasOutcome,
    AlterClientQuotasTurn, host::ALTER_CLIENT_QUOTAS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterClientQuotasHost::new(ports.alter_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota alterations: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        ALTER_CLIENT_QUOTAS_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan("bob"),),
        Err(AlterClientQuotasAdmissionErrorKind::RetainedBytes)
    ));

    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(
        submitted_plan.entries()[0].entity().components()[0].entity_type(),
        "user"
    );
    assert_eq!(
        submitted_plan.entries()[0].entity().components()[0].entity_name(),
        Some("alice")
    );
    assert_eq!(
        submitted_plan.entries()[0].operations()[0].kind(),
        AlterClientQuotaOperationKind::Set(4096.0)
    );
    assert!(result_limit > ALTER_CLIENT_QUOTAS_RETAINED_BYTES / 2);
    assert!(result_limit < ALTER_CLIENT_QUOTAS_RETAINED_BYTES);

    drop(admission.observer);
    host.reject_handoff(operation_id, submitted_plan, result_limit)
        .unwrap_or_else(|error| panic!("reject inspected handoff: {error}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_is_definitely_unsent_and_reclaimable() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterClientQuotasHost::new(ports.alter_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota alterations: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let AlterClientQuotasOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), AlterClientQuotasFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), AlterClientQuotasDeliveryStatus::NotSent);

    let _progress = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("reclaim turn: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn handed_off_without_a_returned_call_cannot_forge_recovery_evidence() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterClientQuotasHost::new(ports.alter_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota alterations: {error:?}"));
    let AlterClientQuotasTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(AlterClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_call_and_correlation_plan_remain_retained_when_core_rejects_terminal_fact() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterClientQuotasHost::new(ports.alter_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota alterations: {error:?}"));
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, _deadline, expected, retained_limit) = submission.into_parts();
    host.retain_recovered_call_for_test(expected.clone(), retained_limit);

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(AlterClientQuotasHostError::Machine(
            AlterClientQuotasMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_ownership_matches_for_test(&expected, retained_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(AlterClientQuotasHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_call_and_correlation_plan_until_post_driver_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = AlterClientQuotasHost::new(ports.alter_client_quotas);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan("alice"))
        .unwrap_or_else(|error| panic!("admit quota alterations: {error:?}"));
    let AlterClientQuotasTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = AlterClientQuotasCall::submit(
        &driver,
        submitted_plan,
        result_limit,
        submitted_deadline.transport(),
    )
    .unwrap_or_else(|_error| panic!("accepted call"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(capture.now()),
        Err(AlterClientQuotasHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let AlterClientQuotasOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(failure.kind(), AlterClientQuotasFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        AlterClientQuotasDeliveryStatus::PossiblySent
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(name: &str) -> AlterClientQuotasPlan {
    AlterClientQuotasPlan::new(
        vec![AlterClientQuotaEntry::new(
            AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                "user".to_owned(),
                Some(name.to_owned()),
            )]),
            vec![
                AlterClientQuotaOperation::set("producer_byte_rate".to_owned(), 4096.0),
                AlterClientQuotaOperation::remove("request_percentage".to_owned()),
            ],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid alteration plan: {error}"))
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
