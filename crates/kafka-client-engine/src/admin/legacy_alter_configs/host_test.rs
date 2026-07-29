//! Bounded API 33 admission, recovery, and retained-byte scenarios.

use std::time::Instant;

use kafka_client_core::{
    LegacyAlterConfigsPlan, LegacyConfigEntry as CoreConfigEntry, LegacyTopicConfigReplacement,
};

use crate::clock::OperationDeadline;

use super::{
    LegacyAlterConfigsAdmissionErrorKind, LegacyAlterConfigsDeliveryStatus,
    LegacyAlterConfigsFailureKind, LegacyAlterConfigsHost, LegacyAlterConfigsOutcome,
    LegacyAlterConfigsTurn, host::LEGACY_ALTER_CONFIGS_CAPACITY,
    model::LegacyAlterConfigsRetention,
};

#[test]
fn admission_reserves_completion_bytes_and_original_deadline_before_start() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let deadline = deadline(10);
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline,
            plan(),
            retention(16 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit legacy configs: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    let LegacyAlterConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline.core(), deadline.core());
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, 8 * 1024);

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn operation_and_completion_capacity_reject_before_an_extra_machine() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let mut observers = Vec::new();
    for _ in 0..LEGACY_ALTER_CONFIGS_CAPACITY {
        let admission = host
            .try_admit(
                kafka_client_core::Moment::from_tick(1),
                deadline(10),
                plan(),
                retention(1),
            )
            .unwrap_or_else(|error| panic!("bounded slot should admit: {error:?}"));
        observers.push(admission.observer);
    }
    assert!(matches!(
        host.try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            retention(1),
        ),
        Err(LegacyAlterConfigsAdmissionErrorKind::Capacity)
    ));
    assert_eq!(host.unsettled(), LEGACY_ALTER_CONFIGS_CAPACITY);

    drop((observers, host));
    stop_notifier(&mut notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_not_sent() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            retention(16 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit legacy configs: {error:?}"));

    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recover host: {error}"));
    let LegacyAlterConfigsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe recovery: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        (failure.kind(), failure.delivery()),
        (
            LegacyAlterConfigsFailureKind::DriverRejected,
            LegacyAlterConfigsDeliveryStatus::NotSent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

fn plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::new(
        vec![LegacyTopicConfigReplacement::new(
            "orders".to_owned(),
            vec![CoreConfigEntry::new(
                "cleanup.policy".to_owned(),
                Some("compact".to_owned()),
            )],
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}

fn retention(total: usize) -> LegacyAlterConfigsRetention {
    LegacyAlterConfigsRetention::from_parts(total, 8 * 1024)
}

fn stop_notifier(notifier: &mut crate::admin::AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
