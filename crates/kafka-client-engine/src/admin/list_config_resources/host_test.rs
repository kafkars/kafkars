//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use kafka_client_core::{ConfigResourceType, ListConfigResourcesPlan};

use crate::{admin::AdminCompletionNotifier, clock::MonotonicClock};

use super::{
    ListConfigResourcesAdmissionErrorKind, ListConfigResourcesDeliveryStatus,
    ListConfigResourcesFailureKind, ListConfigResourcesHost, ListConfigResourcesOutcome,
    ListConfigResourcesTurn,
    host::{LIST_CONFIG_RESOURCES_RESULT_BYTES, LIST_CONFIG_RESOURCES_RETAINED_BYTES},
};

#[test]
fn admission_reserves_terminal_request_and_two_mib_result_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = ListConfigResourcesHost::new(ports.list_config_resources);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit resource query: {error:?}"));
    assert!(admission.fault.is_none());
    let operation_bytes = host.retained_bytes_for_test();
    assert!(operation_bytes > LIST_CONFIG_RESOURCES_RESULT_BYTES);
    assert!(operation_bytes < LIST_CONFIG_RESOURCES_RETAINED_BYTES);
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline(), plan()),
        Err(ListConfigResourcesAdmissionErrorKind::RetainedBytes)
    ));

    let ListConfigResourcesTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert_eq!(
        submitted_plan
            .resource_types()
            .iter()
            .map(|resource_type| resource_type.code())
            .collect::<Vec<_>>(),
        [2, 64]
    );
    assert_eq!(result_limit, LIST_CONFIG_RESOURCES_RESULT_BYTES);

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
    let mut host = ListConfigResourcesHost::new(ports.list_config_resources);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit resource query: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let ListConfigResourcesOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        ListConfigResourcesFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        ListConfigResourcesDeliveryStatus::NotSent
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
    let mut host = ListConfigResourcesHost::new(ports.list_config_resources);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan())
        .unwrap_or_else(|error| panic!("admit resource query: {error:?}"));
    let ListConfigResourcesTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("handoff turn: {error}"))
    else {
        panic!("submission expected");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handoff: {error}"));
    let ListConfigResourcesOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), ListConfigResourcesFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        ListConfigResourcesDeliveryStatus::PossiblySent
    );
    drop(host);
    stop_notifier(&mut notifier);
}

fn plan() -> ListConfigResourcesPlan {
    ListConfigResourcesPlan::new(vec![
        ConfigResourceType::TOPIC,
        ConfigResourceType::new(64).unwrap_or_else(|error| panic!("future type: {error}")),
    ])
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
