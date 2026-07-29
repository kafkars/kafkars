//! Bounded deadline, terminal, and byte-release scenarios for `DescribeConfigs`.

use std::time::Instant;

use kafka_client_core::{DeliveryStatus, DescribeConfigsInput, DescribeConfigsPlan};

use crate::clock::OperationDeadline;

use super::{
    DescribeConfigsDeliveryStatus, DescribeConfigsFailureKind, DescribeConfigsOutcome,
    DescribeConfigsResourceQuery, DescribeConfigsRetention, DescribeConfigsTurn,
    host::DESCRIBE_CONFIGS_RETAINED_BYTES,
};

fn plan() -> DescribeConfigsPlan {
    DescribeConfigsPlan::new(
        vec![kafka_client_core::DescribeConfigsResourceQuery::new(
            2,
            "orders".to_owned(),
            None,
        )],
        false,
        false,
    )
    .unwrap_or_else(|error| panic!("valid DescribeConfigs plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

#[test]
fn terminal_bytes_remain_reserved_until_observer_reclamation() {
    let (mut host, notifier) = crate::admin::test_support::describe_configs_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            DescribeConfigsRetention::from_parts(16 * 1024, 256 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit DescribeConfigs: {error:?}"));
    let DescribeConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must be ready");
    };
    host.apply(
        submission.operation_id,
        DescribeConfigsInput::DriverAccepted,
    )
    .unwrap_or_else(|error| panic!("apply driver acceptance: {error}"));
    host.apply(
        submission.operation_id,
        DescribeConfigsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    )
    .unwrap_or_else(|error| panic!("apply terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);
    let DescribeConfigsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error}"))
    else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), DescribeConfigsFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeConfigsDeliveryStatus::PossiblySent
    );
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 0);
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn retained_byte_limit_rejects_before_completion_reservation() {
    let (mut host, notifier) = crate::admin::test_support::describe_configs_host();
    assert!(matches!(
        host.try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            DescribeConfigsRetention::from_parts(DESCRIBE_CONFIGS_RETAINED_BYTES + 1, 1),
        ),
        Err(super::DescribeConfigsAdmissionErrorKind::RetainedBytes)
    ));
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovery_after_submission_handoff_is_conservatively_possibly_sent() {
    let (mut host, notifier) = crate::admin::test_support::describe_configs_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            DescribeConfigsRetention::from_parts(16 * 1024, 256 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit DescribeConfigs: {error:?}"));
    let DescribeConfigsTurn::Submit(_submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission must cross the handoff boundary");
    };
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover handed-off submission: {error}"));
    let DescribeConfigsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery terminal: {error}"))
    else {
        panic!("transport failure expected");
    };
    assert_eq!(failure.kind(), DescribeConfigsFailureKind::Transport);
    assert_eq!(
        failure.delivery(),
        DescribeConfigsDeliveryStatus::PossiblySent
    );
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn recovery_of_untouched_submission_remains_not_sent() {
    let (mut host, notifier) = crate::admin::test_support::describe_configs_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            DescribeConfigsRetention::from_parts(16 * 1024, 256 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit DescribeConfigs: {error:?}"));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover queued submission: {error}"));
    let DescribeConfigsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery terminal: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(failure.kind(), DescribeConfigsFailureKind::DriverRejected);
    assert_eq!(failure.delivery(), DescribeConfigsDeliveryStatus::NotSent);
    let _progress = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));
    drop(host);
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn public_query_type_remains_distinct_from_core_policy_type() {
    let query = DescribeConfigsResourceQuery::new(2, "orders".to_owned(), None);
    assert!(
        super::DescribeConfigsRequest::new(vec![query], false, false)
            .into_plan()
            .is_ok()
    );
}
