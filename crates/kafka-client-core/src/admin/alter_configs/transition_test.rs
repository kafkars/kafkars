//! Scenarios for incremental configuration lifecycle and terminal ownership.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    ConfigAlteration, IncrementalAlterConfigBrokerError, IncrementalAlterConfigOutcome,
    IncrementalAlterConfigResult, IncrementalAlterConfigsBatch, IncrementalAlterConfigsEffect,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsInput,
    IncrementalAlterConfigsMachine, IncrementalAlterConfigsMachineError,
    IncrementalAlterConfigsPlan, IncrementalAlterConfigsRoute, IncrementalAlterConfigsState,
    IncrementalAlterConfigsTerminal, IncrementalAlterConfigsTransition,
    IncrementalConfigResourceAlteration, TopicConfigAlteration,
};

#[test]
fn single_route_plan_preserves_original_deadline_and_semantics() {
    let mut machine = machine(20);
    let transition = machine
        .apply(IncrementalAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(IncrementalAlterConfigsEffect::Submit {
        operation_id,
        deadline,
        route,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(12));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(route, IncrementalAlterConfigsRoute::AnyBroker);
    assert_eq!(plan, plan_fixture());
    assert_eq!(
        machine.state(),
        IncrementalAlterConfigsState::AwaitingDriver
    );
    assert_eq!(
        machine.apply(IncrementalAlterConfigsInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(IncrementalAlterConfigsMachineError::InvalidState)
    );
}

#[test]
fn ordered_terminal_is_single_assignment_and_retains_exact_broker_facts() {
    let mut machine = submitted_machine();
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = IncrementalAlterConfigsBatch::new(
        77,
        vec![
            IncrementalAlterConfigOutcome::altered("orders"),
            IncrementalAlterConfigOutcome::failed(
                "audit",
                IncrementalAlterConfigBrokerError::new(code, Some("future error".to_owned()), true),
            ),
        ],
    );
    let transition = machine
        .apply(IncrementalAlterConfigsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(IncrementalAlterConfigsEffect::Complete {
        terminal: IncrementalAlterConfigsTerminal::Configs(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 77);
    let IncrementalAlterConfigResult::Failed(error) = batch.topics()[1].result() else {
        panic!("audit must retain its broker failure");
    };
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future error"));
    assert!(error.message_truncated());
    assert_eq!(machine.state(), IncrementalAlterConfigsState::Completed);
    assert_eq!(
        machine.apply(IncrementalAlterConfigsInput::InvalidResponse),
        Err(IncrementalAlterConfigsMachineError::AlreadyCompleted)
    );
}

#[test]
fn malformed_response_settles_invalid_once_without_follow_up() {
    for batch in [
        IncrementalAlterConfigsBatch::new(
            0,
            vec![IncrementalAlterConfigOutcome::altered("orders")],
        ),
        IncrementalAlterConfigsBatch::new(
            0,
            vec![
                IncrementalAlterConfigOutcome::altered("audit"),
                IncrementalAlterConfigOutcome::altered("orders"),
            ],
        ),
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(IncrementalAlterConfigsInput::BrokerResponded { batch })
                .unwrap_or_else(|error| panic!("malformed terminal must settle: {error}")),
            IncrementalAlterConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
        assert_eq!(machine.state(), IncrementalAlterConfigsState::Completed);
        assert_eq!(
            machine.apply(IncrementalAlterConfigsInput::InvalidResponse),
            Err(IncrementalAlterConfigsMachineError::AlreadyCompleted)
        );
    }
}

#[test]
fn resource_correlation_uses_exact_type_name_pairs_in_caller_order() {
    let plan = IncrementalAlterConfigsPlan::for_resources(
        vec![
            IncrementalConfigResourceAlteration::resource(
                4,
                "1".to_owned(),
                vec![ConfigAlteration::delete("broker.key".to_owned())],
            ),
            IncrementalConfigResourceAlteration::resource(
                8,
                "1".to_owned(),
                vec![ConfigAlteration::delete("logger.key".to_owned())],
            ),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid resource plan: {error}"));
    for outcomes in [
        vec![
            IncrementalAlterConfigOutcome::resource_altered(8, "1"),
            IncrementalAlterConfigOutcome::resource_altered(4, "1"),
        ],
        vec![
            IncrementalAlterConfigOutcome::resource_altered(4, "1"),
            IncrementalAlterConfigOutcome::resource_altered(4, "1"),
        ],
    ] {
        let mut machine = IncrementalAlterConfigsMachine::new(
            OperationId::from_raw(41),
            Deadline::from_tick(20),
            plan.clone(),
        );
        machine
            .apply(IncrementalAlterConfigsInput::Start {
                now: Moment::from_tick(1),
            })
            .and_then(|_| machine.apply(IncrementalAlterConfigsInput::DriverAccepted))
            .unwrap_or_else(|error| panic!("submit generic machine: {error}"));
        assert_failure(
            machine
                .apply(IncrementalAlterConfigsInput::BrokerResponded {
                    batch: IncrementalAlterConfigsBatch::new(0, outcomes),
                })
                .unwrap_or_else(|error| panic!("correlation failure settles: {error}")),
            IncrementalAlterConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn pre_driver_failures_are_definitely_not_sent() {
    let mut expired = machine(4);
    assert_failure(
        expired
            .apply(IncrementalAlterConfigsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        IncrementalAlterConfigsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(IncrementalAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(IncrementalAlterConfigsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        IncrementalAlterConfigsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn submitted_failures_preserve_certainty_without_retry_or_legacy_fallback() {
    for (input, kind, delivery) in [
        (
            IncrementalAlterConfigsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            IncrementalAlterConfigsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            IncrementalAlterConfigsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            IncrementalAlterConfigsFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            IncrementalAlterConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            IncrementalAlterConfigsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            IncrementalAlterConfigsInput::ResponseTooLarge,
            IncrementalAlterConfigsFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            IncrementalAlterConfigsInput::InvalidResponse,
            IncrementalAlterConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("failure should settle: {error}"));
        assert_failure(transition, kind, delivery);
        assert_eq!(machine.state(), IncrementalAlterConfigsState::Completed);
    }
}

fn machine(deadline: u64) -> IncrementalAlterConfigsMachine {
    IncrementalAlterConfigsMachine::new(
        OperationId::from_raw(12),
        Deadline::from_tick(deadline),
        plan_fixture(),
    )
}

fn submitted_machine() -> IncrementalAlterConfigsMachine {
    let mut machine = machine(20);
    machine
        .apply(IncrementalAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(IncrementalAlterConfigsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn plan_fixture() -> IncrementalAlterConfigsPlan {
    IncrementalAlterConfigsPlan::new(
        vec![
            TopicConfigAlteration::new(
                "orders".to_owned(),
                vec![
                    ConfigAlteration::set("retention.ms".to_owned(), "10".to_owned()),
                    ConfigAlteration::delete("segment.ms".to_owned()),
                ],
            ),
            TopicConfigAlteration::new(
                "audit".to_owned(),
                vec![ConfigAlteration::append(
                    "cleanup.policy".to_owned(),
                    "compact".to_owned(),
                )],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn assert_failure(
    transition: IncrementalAlterConfigsTransition,
    expected_kind: IncrementalAlterConfigsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let Some(IncrementalAlterConfigsEffect::Complete {
        terminal: IncrementalAlterConfigsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
