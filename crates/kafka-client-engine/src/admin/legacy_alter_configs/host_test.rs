//! Bounded API 33 admission, handoff, and retained-byte scenarios.

use std::time::Instant;

use kafka_client_core::{
    LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, LegacyConfigEntry as CoreConfigEntry,
    LegacyTopicConfigReplacement,
};

use crate::clock::OperationDeadline;

use super::{
    LegacyAlterConfigsAdmissionErrorKind, LegacyAlterConfigsDeliveryStatus,
    LegacyAlterConfigsFailureKind, LegacyAlterConfigsHost, LegacyAlterConfigsHostError,
    LegacyAlterConfigsOutcome, LegacyAlterConfigsTurn, host::LEGACY_ALTER_CONFIGS_CAPACITY,
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
    let (_id, submitted_deadline, route, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline.core(), deadline.core());
    assert_eq!(route, LegacyAlterConfigsRoute::AnyBroker);
    assert_eq!(submitted_plan, plan());
    assert_eq!(result_limit, result_limit_for(&plan()));

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
fn rejected_handoff_requires_the_exact_route_and_ordered_plan() {
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
    let LegacyAlterConfigsTurn::Submit(submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, route, plan, _result_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(
            operation_id,
            LegacyAlterConfigsRoute::ExactBroker(7),
            plan.clone(),
        ),
        Err(LegacyAlterConfigsHostError::SubmissionMismatch)
    ));
    host.reject_handoff(operation_id, route, plan)
        .unwrap_or_else(|error| panic!("reject exact handoff: {error}"));
    let LegacyAlterConfigsOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
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
    LegacyAlterConfigsRetention::from_parts(total, result_limit_for(&plan()))
}

fn result_limit_for(plan: &LegacyAlterConfigsPlan) -> usize {
    super::model::legacy_alter_configs_result_limit(plan)
        .unwrap_or_else(|| panic!("small result limit fits"))
}

fn stop_notifier(notifier: &mut crate::admin::AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
