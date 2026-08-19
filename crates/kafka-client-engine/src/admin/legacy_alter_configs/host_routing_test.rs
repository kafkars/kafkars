//! Serial-route handoff and recovery scenarios for legacy `AlterConfigs`.

use std::time::Instant;

use kafka_client_core::{
    LegacyAlterConfigOutcome, LegacyAlterConfigsBatch, LegacyAlterConfigsInput,
    LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, LegacyConfigResourceReplacement,
};

use crate::clock::OperationDeadline;

use super::{
    LegacyAlterConfigsDeliveryStatus, LegacyAlterConfigsHost, LegacyAlterConfigsOutcome,
    LegacyAlterConfigsTurn, model::LegacyAlterConfigsRetention,
};

#[test]
fn later_route_recovery_preserves_prior_possible_delivery_at_every_handoff_boundary() {
    for phase in [
        LaterRoutePhase::Untouched,
        LaterRoutePhase::HandedOff,
        LaterRoutePhase::Submitted,
    ] {
        let plan = mixed_plan();
        let (mut notifier, ports) = crate::admin::test_support::completion_owner();
        let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
        let admission = host
            .try_admit(
                kafka_client_core::Moment::from_tick(1),
                deadline(10),
                plan.clone(),
                LegacyAlterConfigsRetention::from_parts(32 * 1024, result_limit_for(&plan)),
            )
            .unwrap_or_else(|error| panic!("admit mixed configs: {error:?}"));
        let LegacyAlterConfigsTurn::Submit(first) = host
            .turn(kafka_client_core::Moment::from_tick(2))
            .unwrap_or_else(|error| panic!("take first route: {error}"))
        else {
            panic!("first route expected");
        };
        let (first_id, _deadline, first_route, _plan, _limit) = first.into_parts();
        assert_eq!(first_route, LegacyAlterConfigsRoute::AnyBroker);
        host.apply_input_for_test(first_id, LegacyAlterConfigsInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept first route: {error}"));
        host.apply_response_for_test(
            first_id,
            LegacyAlterConfigsBatch::new(
                3,
                vec![LegacyAlterConfigOutcome::resource_altered(2, "orders")],
            ),
        )
        .unwrap_or_else(|error| panic!("settle first route: {error}"));

        if phase != LaterRoutePhase::Untouched {
            let LegacyAlterConfigsTurn::Submit(second) = host
                .turn(kafka_client_core::Moment::from_tick(3))
                .unwrap_or_else(|error| panic!("take second route: {error}"))
            else {
                panic!("second route expected");
            };
            let (second_id, _deadline, second_route, second_plan, _limit) = second.into_parts();
            assert_eq!(second_route, LegacyAlterConfigsRoute::ExactBroker(1));
            if phase == LaterRoutePhase::Submitted {
                host.apply_input_for_test(second_id, LegacyAlterConfigsInput::DriverAccepted)
                    .unwrap_or_else(|error| panic!("accept second route: {error}"));
            }
            host.retain_recovered_call_for_test(second_route, second_plan);
        }

        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("recover later route: {error}"));
        let LegacyAlterConfigsOutcome::Failed(failure) = admission
            .observer
            .wait()
            .unwrap_or_else(|error| panic!("observe recovery: {error}"))
        else {
            panic!("recovery failure expected");
        };
        assert_eq!(
            failure.delivery(),
            LegacyAlterConfigsDeliveryStatus::PossiblySent
        );
        drop(host);
        stop_notifier(&mut notifier);
    }
}

#[test]
fn serial_success_consumes_disjoint_contributions_and_publishes_caller_order() {
    let plan = mixed_plan();
    let (mut notifier, ports) = crate::admin::test_support::completion_owner();
    let mut host = LegacyAlterConfigsHost::new(ports.legacy_alter_configs);
    let admission = host
        .try_admit(
            kafka_client_core::Moment::from_tick(1),
            deadline(10),
            plan.clone(),
            LegacyAlterConfigsRetention::from_parts(32 * 1024, result_limit_for(&plan)),
        )
        .unwrap_or_else(|error| panic!("admit mixed configs: {error:?}"));

    let LegacyAlterConfigsTurn::Submit(first) = host
        .turn(kafka_client_core::Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take first route: {error}"))
    else {
        panic!("first route expected");
    };
    let (operation_id, original_deadline, first_route, _plan, _limit) = first.into_parts();
    assert_eq!(original_deadline.core(), deadline(10).core());
    assert_eq!(first_route, LegacyAlterConfigsRoute::AnyBroker);
    host.apply_input_for_test(operation_id, LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept first route: {error}"));
    host.apply_response_for_test(
        operation_id,
        LegacyAlterConfigsBatch::new(
            3,
            vec![LegacyAlterConfigOutcome::resource_altered(2, "orders")],
        ),
    )
    .unwrap_or_else(|error| panic!("settle first route: {error}"));

    let LegacyAlterConfigsTurn::Submit(second) = host
        .turn(kafka_client_core::Moment::from_tick(3))
        .unwrap_or_else(|error| panic!("take second route: {error}"))
    else {
        panic!("second route expected");
    };
    let (second_id, second_deadline, second_route, _plan, _limit) = second.into_parts();
    assert_eq!(second_id, operation_id);
    assert_eq!(second_deadline.core(), original_deadline.core());
    assert_eq!(second_route, LegacyAlterConfigsRoute::ExactBroker(1));
    host.apply_input_for_test(second_id, LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second route: {error}"));
    host.apply_response_for_test(
        second_id,
        LegacyAlterConfigsBatch::new(7, vec![LegacyAlterConfigOutcome::resource_altered(4, "1")]),
    )
    .unwrap_or_else(|error| panic!("settle second route: {error}"));

    let LegacyAlterConfigsOutcome::Configs(result) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe configs: {error}"))
    else {
        panic!("successful configs expected");
    };
    assert_eq!(result.throttle_time_ms(), 7);
    assert_eq!(
        result
            .resources()
            .iter()
            .map(super::outcome::LegacyAlterConfigResult::resource_name)
            .collect::<Vec<_>>(),
        ["orders", "1"]
    );
    drop(host);
    stop_notifier(&mut notifier);
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LaterRoutePhase {
    Untouched,
    HandedOff,
    Submitted,
}

fn mixed_plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::for_resources(
        vec![
            LegacyConfigResourceReplacement::resource(2, "orders".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(4, "1".to_owned(), Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid mixed plan: {error}"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
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
