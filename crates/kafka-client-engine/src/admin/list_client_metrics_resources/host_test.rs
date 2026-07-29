//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use crate::{admin::AdminCompletionNotifier, clock::MonotonicClock};

use super::{
    ListClientMetricsResourcesAdmissionErrorKind, ListClientMetricsResourcesDeliveryStatus,
    ListClientMetricsResourcesFailureKind, ListClientMetricsResourcesHost,
    ListClientMetricsResourcesOutcome, ListClientMetricsResourcesTurn,
    host::LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ListClientMetricsResourcesHost::new(ports.list_client_metrics_resources);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline())
        .unwrap_or_else(|error| panic!("admit resource query: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline()),
        Err(ListClientMetricsResourcesAdmissionErrorKind::RetainedBytes)
    ));

    let ListClientMetricsResourcesTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert!(result_limit > LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES / 2);
    assert!(result_limit < LIST_CLIENT_METRICS_RESOURCES_RETAINED_BYTES);

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
    let mut host = ListClientMetricsResourcesHost::new(ports.list_client_metrics_resources);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline())
        .unwrap_or_else(|error| panic!("admit resource query: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let ListClientMetricsResourcesOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        ListClientMetricsResourcesFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        ListClientMetricsResourcesDeliveryStatus::NotSent
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
    let mut host = ListClientMetricsResourcesHost::new(ports.list_client_metrics_resources);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline())
        .unwrap_or_else(|error| panic!("admit resource query: {error:?}"));
    let ListClientMetricsResourcesTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handoff: {error}"));
    let ListClientMetricsResourcesOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        ListClientMetricsResourcesFailureKind::Transport
    );
    assert_eq!(
        failure.delivery(),
        ListClientMetricsResourcesDeliveryStatus::PossiblySent
    );
    drop(host);
    stop_notifier(&mut notifier);
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
