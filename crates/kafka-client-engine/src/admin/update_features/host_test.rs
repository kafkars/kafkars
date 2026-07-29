//! Admission, deadline, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{
    UpdateFeature as CoreFeature, UpdateFeatureIntent as CoreIntent, UpdateFeaturesPlan,
};

use crate::{admin::AdminCompletionNotifier, clock::MonotonicClock};

use super::{
    UpdateFeaturesAdmissionErrorKind, UpdateFeaturesDeliveryStatus, UpdateFeaturesFailureKind,
    UpdateFeaturesHost, UpdateFeaturesOutcome, UpdateFeaturesTurn,
    host::UPDATE_FEATURES_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UpdateFeaturesHost::new(ports.update_features);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(false))
        .unwrap_or_else(|error| panic!("admit feature update: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        UPDATE_FEATURES_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan(false),),
        Err(UpdateFeaturesAdmissionErrorKind::RetainedBytes)
    ));

    let UpdateFeaturesTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(submitted_plan, plan(false));
    assert!(result_limit > UPDATE_FEATURES_RETAINED_BYTES / 2);
    assert!(result_limit < UPDATE_FEATURES_RETAINED_BYTES);

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
    let mut host = UpdateFeaturesHost::new(ports.update_features);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(false))
        .unwrap_or_else(|error| panic!("admit feature update: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched update: {error}"));
    let UpdateFeaturesOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), &UpdateFeaturesFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), UpdateFeaturesDeliveryStatus::NotSent);

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
    let mut host = UpdateFeaturesHost::new(ports.update_features);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(false))
        .unwrap_or_else(|error| panic!("admit feature update: {error:?}"));
    let UpdateFeaturesTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handoff: {error}"));
    let UpdateFeaturesOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), &UpdateFeaturesFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        UpdateFeaturesDeliveryStatus::PossiblySent
    );
    drop(host);
    stop_notifier(&mut notifier);
}

fn plan(validate_only: bool) -> UpdateFeaturesPlan {
    UpdateFeaturesPlan::new(
        vec![CoreFeature::new(
            "metadata.version".to_owned(),
            7,
            CoreIntent::Upgrade,
        )],
        validate_only,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
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
