//! Bounded admission, recovery certainty, and exact byte-release scenarios.

use std::time::Instant;

use kafka_client_core::{
    ConfigAlteration, DeliveryStatus, IncrementalAlterConfigsInput, IncrementalAlterConfigsPlan,
    IncrementalAlterConfigsRoute, TopicConfigAlteration,
};

use crate::clock::OperationDeadline;

use super::{
    IncrementalAlterConfigsAdmissionErrorKind, IncrementalAlterConfigsDeliveryStatus,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsOutcome,
    IncrementalAlterConfigsTurn, host::INCREMENTAL_ALTER_CONFIGS_CAPACITY,
    model::IncrementalAlterConfigsRetention,
};

#[test]
fn admission_reserves_slot_completion_bytes_and_original_deadline_before_machine_start() {
    let (mut host, notifier) = crate::admin::test_support::incremental_alter_configs_host();
    let deadline = deadline(10);
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline,
            plan(),
            retention(16 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit incremental configs: {error:?}"));
    assert!(admission.fault.is_none());
    assert_eq!(host.unsettled(), 1);
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    let IncrementalAlterConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (_id, submitted_deadline, route, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline.core(), deadline.core());
    assert_eq!(route, IncrementalAlterConfigsRoute::AnyBroker);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, result_limit_for(&plan()));

    drop((admission, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn operation_and_completion_capacity_reject_before_constructing_an_extra_machine() {
    let (mut host, notifier) = crate::admin::test_support::incremental_alter_configs_host();
    let mut observers = Vec::new();
    for _ in 0..INCREMENTAL_ALTER_CONFIGS_CAPACITY {
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
        Err(IncrementalAlterConfigsAdmissionErrorKind::Capacity)
    ));
    assert_eq!(host.unsettled(), INCREMENTAL_ALTER_CONFIGS_CAPACITY);
    assert_eq!(
        host.retained_bytes_for_test(),
        INCREMENTAL_ALTER_CONFIGS_CAPACITY
    );

    drop((observers, host));
    crate::admin::test_support::stop_notifier(notifier);
}

#[test]
fn abandoned_observer_releases_reserved_bytes_once_after_terminal_publication() {
    let (mut host, notifier) = crate::admin::test_support::incremental_alter_configs_host();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan(),
            retention(16 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit incremental configs: {error:?}"));
    let IncrementalAlterConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    host.apply(
        submission.operation_id,
        IncrementalAlterConfigsInput::DriverAccepted,
    )
    .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
    drop(admission.observer);
    host.apply(
        submission.operation_id,
        IncrementalAlterConfigsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        },
    )
    .unwrap_or_else(|error| panic!("terminal publication: {error}"));
    assert_eq!(host.retained_bytes_for_test(), 16 * 1024);
    crate::admin::test_support::stop_notifier(notifier);
    assert!(matches!(
        host.turn(kafka_client_core::Moment::from_tick(3)),
        Ok(IncrementalAlterConfigsTurn::Progress)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);
    assert!(matches!(
        host.turn(kafka_client_core::Moment::from_tick(4)),
        Ok(IncrementalAlterConfigsTurn::Idle)
    ));
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
}

#[test]
fn recovery_distinguishes_untouched_handed_off_and_submitted_work() {
    for phase in [
        RecoveryPhase::Untouched,
        RecoveryPhase::HandedOff,
        RecoveryPhase::Submitted,
    ] {
        let (mut host, notifier) = crate::admin::test_support::incremental_alter_configs_host();
        let admission = host
            .try_admit(
                kafka_client_core::Moment::from_tick(1),
                deadline(10),
                plan(),
                retention(16 * 1024),
            )
            .unwrap_or_else(|error| panic!("admit incremental configs: {error:?}"));
        if phase != RecoveryPhase::Untouched {
            let IncrementalAlterConfigsTurn::Submit(submission) = host
                .turn(kafka_client_core::Moment::from_tick(2))
                .unwrap_or_else(|error| panic!("take submission: {error}"))
            else {
                panic!("submission expected");
            };
            if phase == RecoveryPhase::Submitted {
                host.apply(
                    submission.operation_id,
                    IncrementalAlterConfigsInput::DriverAccepted,
                )
                .unwrap_or_else(|error| panic!("driver acceptance: {error}"));
            }
        }
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover host: {error}"));
        let IncrementalAlterConfigsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("recovery failure expected");
        };
        let expected = match phase {
            RecoveryPhase::Untouched => (
                IncrementalAlterConfigsFailureKind::DriverRejected,
                IncrementalAlterConfigsDeliveryStatus::NotSent,
            ),
            RecoveryPhase::HandedOff | RecoveryPhase::Submitted => (
                IncrementalAlterConfigsFailureKind::Transport,
                IncrementalAlterConfigsDeliveryStatus::PossiblySent,
            ),
        };
        assert_eq!((failure.kind(), failure.delivery()), expected);
        let _progress = host
            .turn(kafka_client_core::Moment::from_tick(3))
            .unwrap_or_else(|error| panic!("reclaim terminal: {error}"));

        drop(host);
        crate::admin::test_support::stop_notifier(notifier);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RecoveryPhase {
    Untouched,
    HandedOff,
    Submitted,
}

fn plan() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::new(
        vec![TopicConfigAlteration::new(
            "orders".to_owned(),
            vec![ConfigAlteration::set(
                "cleanup.policy".to_owned(),
                "compact".to_owned(),
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

fn retention(total: usize) -> IncrementalAlterConfigsRetention {
    IncrementalAlterConfigsRetention::from_parts(total, result_limit_for(&plan()))
}

fn result_limit_for(plan: &IncrementalAlterConfigsPlan) -> usize {
    super::model::incremental_alter_configs_result_limit(plan)
        .unwrap_or_else(|| panic!("small result limit fits"))
}
