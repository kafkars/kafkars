//! Admission, shutdown recovery, and retained-envelope scenarios.

use std::sync::Arc;

use crate::{
    admin::{AdminCompletionNotifier, DescribeMetadataQuorumHost},
    clock::MonotonicClock,
};

use super::{
    DescribeMetadataQuorumAdmissionErrorKind, DescribeMetadataQuorumDeliveryStatus,
    DescribeMetadataQuorumFailureKind, DescribeMetadataQuorumOutcome, DescribeMetadataQuorumTurn,
    host::DESCRIBE_METADATA_QUORUM_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_full_envelope_before_submission() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = DescribeMetadataQuorumHost::new(ports.describe_metadata_quorum);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline())
        .unwrap_or_else(|error| panic!("admit quorum query: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        DESCRIBE_METADATA_QUORUM_RETAINED_BYTES
    );
    assert_eq!(
        host.next_deadline(),
        Some(capture.operation_deadline().core())
    );
    assert!(matches!(
        host.try_admit(capture.now(), capture.operation_deadline()),
        Err(DescribeMetadataQuorumAdmissionErrorKind::RetainedBytes)
    ));

    let DescribeMetadataQuorumTurn::Submit(submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, capture.operation_deadline());
    assert!(result_limit > DESCRIBE_METADATA_QUORUM_RETAINED_BYTES / 2);
    assert!(result_limit < DESCRIBE_METADATA_QUORUM_RETAINED_BYTES);

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
    let mut host = DescribeMetadataQuorumHost::new(ports.describe_metadata_quorum);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline())
        .unwrap_or_else(|error| panic!("admit quorum query: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover untouched query: {error}"));
    let DescribeMetadataQuorumOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(
        failure.kind(),
        DescribeMetadataQuorumFailureKind::DriverRejected
    );
    assert_eq!(
        failure.delivery(),
        DescribeMetadataQuorumDeliveryStatus::NotSent
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
    let mut host = DescribeMetadataQuorumHost::new(ports.describe_metadata_quorum);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline())
        .unwrap_or_else(|error| panic!("admit quorum query: {error:?}"));
    let DescribeMetadataQuorumTurn::Submit(_submission) = host
        .turn(capture.now())
        .unwrap_or_else(|error| panic!("hand off submission: {error}"))
    else {
        panic!("submission expected");
    };

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handoff: {error}"));
    let DescribeMetadataQuorumOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("failure expected");
    };
    assert_eq!(failure.kind(), DescribeMetadataQuorumFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeMetadataQuorumDeliveryStatus::PossiblySent
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
