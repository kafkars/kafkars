//! Untouched, missing-call, correlation, and completion-fault recovery scenarios.

use std::time::Instant;

use kafka_client_core::{
    LegacyAlterConfigsMachineError, LegacyAlterConfigsPlan, LegacyAlterConfigsRoute,
    LegacyConfigEntry as CoreConfigEntry, LegacyTopicConfigReplacement,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{DriverOwner, LegacyAlterConfigsCall},
};

use super::super::super::{
    LegacyAlterConfigsDeliveryStatus, LegacyAlterConfigsFailureKind, LegacyAlterConfigsHost,
    LegacyAlterConfigsHostError, LegacyAlterConfigsOutcome, LegacyAlterConfigsTurn,
    model::LegacyAlterConfigsRetention,
};

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

#[test]
fn handed_off_without_returned_call_cannot_forge_recovery_ownership() {
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
    let LegacyAlterConfigsTurn::Submit(_submission) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(LegacyAlterConfigsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn recovered_route_and_ordered_plan_survive_core_rejection() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let expected = plan();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            expected.clone(),
            retention(16 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit legacy configs: {error:?}"));
    host.retain_recovered_call_for_test(LegacyAlterConfigsRoute::AnyBroker, expected.clone());

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(LegacyAlterConfigsHostError::Machine(
            LegacyAlterConfigsMachineError::InvalidState
        ))
    ));
    assert!(
        host.recovered_correlation_matches_for_test(LegacyAlterConfigsRoute::AnyBroker, &expected,)
    );
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(LegacyAlterConfigsHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn completion_fault_retains_route_and_ordered_plan_until_shutdown_recovery() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let operation_deadline = deadline(10);
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            operation_deadline,
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
    let (operation_id, deadline, route, plan, _result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = LegacyAlterConfigsCall::submit(&driver, route, plan, deadline.transport())
        .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(kafka_client_core::Moment::from_tick(2)),
        Err(LegacyAlterConfigsHostError::CallCompletion)
    ));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
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
            LegacyAlterConfigsFailureKind::Transport,
            LegacyAlterConfigsDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_recovered_correlation_blocks_core_settlement_and_publication() {
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let expected = plan();
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            expected.clone(),
            retention(16 * 1024),
        )
        .unwrap_or_else(|error| panic!("admit legacy configs: {error:?}"));
    host.retain_recovered_call_for_test(LegacyAlterConfigsRoute::ExactBroker(7), expected.clone());

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(LegacyAlterConfigsHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_correlation_matches_for_test(
        LegacyAlterConfigsRoute::ExactBroker(7),
        &expected,
    ));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(LegacyAlterConfigsHostError::InvalidHandoff)
    ));

    drop((admission, host));
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
    super::super::super::model::legacy_alter_configs_result_limit(plan)
        .unwrap_or_else(|| panic!("small result limit fits"))
}

fn stop_notifier(notifier: &mut crate::admin::AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
