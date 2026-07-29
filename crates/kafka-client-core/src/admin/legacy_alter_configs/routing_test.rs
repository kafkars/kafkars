//! Deterministic mixed-route LegacyAlterConfigs scenarios.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    LegacyAlterConfigOutcome, LegacyAlterConfigsBatch, LegacyAlterConfigsEffect,
    LegacyAlterConfigsFailureKind, LegacyAlterConfigsInput, LegacyAlterConfigsMachine,
    LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, LegacyAlterConfigsState,
    LegacyAlterConfigsTerminal, LegacyAlterConfigsTransition, LegacyConfigResourceReplacement,
};

#[test]
fn mixed_resources_submit_first_seen_routes_and_merge_caller_order() {
    let mut machine = LegacyAlterConfigsMachine::new(
        OperationId::from_raw(71),
        Deadline::from_tick(500),
        mixed_plan(),
    );

    let first = machine
        .apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(7),
        })
        .unwrap_or_else(|error| panic!("start mixed operation: {error}"));
    assert_submit(
        first,
        OperationId::from_raw(71),
        LegacyAlterConfigsRoute::ExactBroker(2),
        &[(4, "2"), (8, "2")],
    );
    machine
        .apply(LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept broker two: {error}"));

    let second = machine
        .apply(LegacyAlterConfigsInput::BrokerResponded {
            batch: batch(11, &[(4, "2"), (8, "2")]),
        })
        .unwrap_or_else(|error| panic!("settle broker two: {error}"));
    assert_submit(
        second,
        OperationId::from_raw(71),
        LegacyAlterConfigsRoute::AnyBroker,
        &[(2, "orders"), (16, "payments-client")],
    );
    machine
        .apply(LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept any broker: {error}"));

    let third = machine
        .apply(LegacyAlterConfigsInput::BrokerResponded {
            batch: batch(17, &[(2, "orders"), (16, "payments-client")]),
        })
        .unwrap_or_else(|error| panic!("settle any broker: {error}"));
    assert_submit(
        third,
        OperationId::from_raw(71),
        LegacyAlterConfigsRoute::ExactBroker(1),
        &[(4, "1"), (8, "1")],
    );
    machine
        .apply(LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept broker one: {error}"));

    let complete = machine
        .apply(LegacyAlterConfigsInput::BrokerResponded {
            batch: batch(5, &[(4, "1"), (8, "1")]),
        })
        .unwrap_or_else(|error| panic!("settle broker one: {error}"));
    let Some(LegacyAlterConfigsEffect::Complete {
        operation_id,
        terminal: LegacyAlterConfigsTerminal::Configs(batch),
    }) = complete.into_effect()
    else {
        panic!("last route must complete");
    };

    assert_eq!(operation_id, OperationId::from_raw(71));
    assert_eq!(batch.throttle_time_ms(), 17);
    assert_eq!(
        batch
            .resources()
            .iter()
            .map(|outcome| (outcome.resource_type(), outcome.resource_name()))
            .collect::<Vec<_>>(),
        [
            (4, "2"),
            (2, "orders"),
            (8, "2"),
            (4, "1"),
            (16, "payments-client"),
            (8, "1"),
        ]
    );
    assert_eq!(machine.state(), LegacyAlterConfigsState::Completed);
}

#[test]
fn later_route_local_not_sent_failures_become_possibly_sent() {
    let mut awaiting = advance_one_successful_route();
    assert_failure(
        awaiting
            .apply(LegacyAlterConfigsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("reject second route: {error}")),
        LegacyAlterConfigsFailureKind::DriverRejected,
        DeliveryStatus::PossiblySent,
    );

    let mut submitted = advance_one_successful_route();
    submitted
        .apply(LegacyAlterConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept second route: {error}"));
    assert_failure(
        submitted
            .apply(LegacyAlterConfigsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            })
            .unwrap_or_else(|error| panic!("fail second route: {error}")),
        LegacyAlterConfigsFailureKind::Transport,
        DeliveryStatus::PossiblySent,
    );
}

fn advance_one_successful_route() -> LegacyAlterConfigsMachine {
    let plan = LegacyAlterConfigsPlan::for_resources(
        vec![resource(4, "1"), resource(2, "orders"), resource(8, "1")],
        false,
    )
    .unwrap_or_else(|error| panic!("valid two-route plan: {error}"));
    let mut machine =
        LegacyAlterConfigsMachine::new(OperationId::from_raw(91), Deadline::from_tick(500), plan);
    machine
        .apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(LegacyAlterConfigsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit first route: {error}"));
    let next = machine
        .apply(LegacyAlterConfigsInput::BrokerResponded {
            batch: batch(3, &[(4, "1"), (8, "1")]),
        })
        .unwrap_or_else(|error| panic!("settle first route: {error}"));
    assert_submit(
        next,
        OperationId::from_raw(91),
        LegacyAlterConfigsRoute::AnyBroker,
        &[(2, "orders")],
    );
    machine
}

fn mixed_plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::for_resources(
        vec![
            resource(4, "2"),
            resource(2, "orders"),
            resource(8, "2"),
            resource(4, "1"),
            resource(16, "payments-client"),
            resource(8, "1"),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid mixed plan: {error}"))
}

fn resource(resource_type: i8, name: &str) -> LegacyConfigResourceReplacement {
    LegacyConfigResourceReplacement::resource(resource_type, name.to_owned(), Vec::new())
}

fn batch(throttle_time_ms: u32, resources: &[(i8, &str)]) -> LegacyAlterConfigsBatch {
    LegacyAlterConfigsBatch::new(
        throttle_time_ms,
        resources
            .iter()
            .map(|(resource_type, name)| {
                LegacyAlterConfigOutcome::resource_altered(*resource_type, *name)
            })
            .collect(),
    )
}

fn assert_submit(
    transition: LegacyAlterConfigsTransition,
    expected_operation_id: OperationId,
    expected_route: LegacyAlterConfigsRoute,
    expected_resources: &[(i8, &str)],
) {
    let Some(LegacyAlterConfigsEffect::Submit {
        operation_id,
        deadline,
        route,
        plan,
    }) = transition.into_effect()
    else {
        panic!("expected route submit");
    };
    assert_eq!(operation_id, expected_operation_id);
    assert_eq!(deadline, Deadline::from_tick(500));
    assert_eq!(route, expected_route);
    assert_eq!(
        plan.resources()
            .iter()
            .map(|resource| (resource.resource_type(), resource.resource_name()))
            .collect::<Vec<_>>(),
        expected_resources
    );
}

fn assert_failure(
    transition: LegacyAlterConfigsTransition,
    expected_kind: LegacyAlterConfigsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let Some(LegacyAlterConfigsEffect::Complete {
        terminal: LegacyAlterConfigsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
