//! Scenarios for legacy full-snapshot configuration lifecycle and terminal ownership.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    LegacyAlterConfigBrokerError, LegacyAlterConfigOutcome, LegacyAlterConfigResult,
    LegacyAlterConfigsBatch, LegacyAlterConfigsEffect, LegacyAlterConfigsFailureKind,
    LegacyAlterConfigsInput, LegacyAlterConfigsMachine, LegacyAlterConfigsMachineError,
    LegacyAlterConfigsPlan, LegacyAlterConfigsRoute, LegacyAlterConfigsState,
    LegacyAlterConfigsTerminal, LegacyAlterConfigsTransition, LegacyConfigEntry,
    LegacyConfigResourceReplacement, LegacyTopicConfigReplacement,
};

#[test]
fn single_route_plan_preserves_original_deadline_and_semantics() {
    let mut machine = machine(20);
    let transition = machine
        .apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(LegacyAlterConfigsEffect::Submit {
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
    assert_eq!(route, LegacyAlterConfigsRoute::AnyBroker);
    assert_eq!(plan, plan_fixture());
    assert_eq!(machine.state(), LegacyAlterConfigsState::AwaitingDriver);
    assert_eq!(
        machine.apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(2),
        }),
        Err(LegacyAlterConfigsMachineError::InvalidState)
    );
}

#[test]
fn ordered_terminal_is_single_assignment_and_retains_exact_broker_facts() {
    let mut machine = submitted_machine();
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = LegacyAlterConfigsBatch::new(
        77,
        vec![
            LegacyAlterConfigOutcome::altered("orders"),
            LegacyAlterConfigOutcome::failed(
                "audit",
                LegacyAlterConfigBrokerError::new(code, Some("future error".to_owned()), true),
            ),
        ],
    );
    let transition = machine
        .apply(LegacyAlterConfigsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(LegacyAlterConfigsEffect::Complete {
        terminal: LegacyAlterConfigsTerminal::Configs(batch),
        ..
    }) = transition.into_effect()
    else {
        panic!("response must complete");
    };

    assert_eq!(batch.throttle_time_ms(), 77);
    let LegacyAlterConfigResult::Failed(error) = batch.topics()[1].result() else {
        panic!("audit must retain its broker failure");
    };
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future error"));
    assert!(error.message_truncated());
    assert_eq!(machine.state(), LegacyAlterConfigsState::Completed);
    assert_eq!(
        machine.apply(LegacyAlterConfigsInput::InvalidResponse),
        Err(LegacyAlterConfigsMachineError::AlreadyCompleted)
    );
}

#[test]
fn malformed_response_settles_invalid_once_without_follow_up() {
    for batch in [
        LegacyAlterConfigsBatch::new(0, vec![LegacyAlterConfigOutcome::altered("orders")]),
        LegacyAlterConfigsBatch::new(
            0,
            vec![
                LegacyAlterConfigOutcome::altered("audit"),
                LegacyAlterConfigOutcome::altered("orders"),
            ],
        ),
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(LegacyAlterConfigsInput::BrokerResponded { batch })
                .unwrap_or_else(|error| panic!("malformed terminal must settle: {error}")),
            LegacyAlterConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
        assert_eq!(machine.state(), LegacyAlterConfigsState::Completed);
        assert_eq!(
            machine.apply(LegacyAlterConfigsInput::InvalidResponse),
            Err(LegacyAlterConfigsMachineError::AlreadyCompleted)
        );
    }
}

#[test]
fn resource_correlation_uses_exact_type_name_pairs_in_caller_order() {
    let plan = LegacyAlterConfigsPlan::for_resources(
        vec![
            LegacyConfigResourceReplacement::resource(4, "1".to_owned(), Vec::new()),
            LegacyConfigResourceReplacement::resource(8, "1".to_owned(), Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid resource plan: {error}"));
    for outcomes in [
        vec![
            LegacyAlterConfigOutcome::resource_altered(8, "1"),
            LegacyAlterConfigOutcome::resource_altered(4, "1"),
        ],
        vec![
            LegacyAlterConfigOutcome::resource_altered(4, "1"),
            LegacyAlterConfigOutcome::resource_altered(4, "1"),
        ],
    ] {
        let mut machine = LegacyAlterConfigsMachine::new(
            OperationId::from_raw(41),
            Deadline::from_tick(20),
            plan.clone(),
        );
        machine
            .apply(LegacyAlterConfigsInput::Start {
                now: Moment::from_tick(1),
            })
            .and_then(|_| machine.apply(LegacyAlterConfigsInput::DriverAccepted))
            .unwrap_or_else(|error| panic!("submit generic machine: {error}"));
        assert_failure(
            machine
                .apply(LegacyAlterConfigsInput::BrokerResponded {
                    batch: LegacyAlterConfigsBatch::new(0, outcomes),
                })
                .unwrap_or_else(|error| panic!("correlation failure settles: {error}")),
            LegacyAlterConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        );
    }
}

#[test]
fn pre_driver_failures_are_definitely_not_sent() {
    let mut expired = machine(4);
    assert_failure(
        expired
            .apply(LegacyAlterConfigsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        LegacyAlterConfigsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(LegacyAlterConfigsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        LegacyAlterConfigsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn submitted_failures_preserve_certainty_without_retry_or_modern_substitution() {
    for (input, kind, delivery) in [
        (
            LegacyAlterConfigsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            LegacyAlterConfigsFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            LegacyAlterConfigsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            },
            LegacyAlterConfigsFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
        (
            LegacyAlterConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            LegacyAlterConfigsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            LegacyAlterConfigsInput::ResponseTooLarge,
            LegacyAlterConfigsFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            LegacyAlterConfigsInput::InvalidResponse,
            LegacyAlterConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        let transition = machine
            .apply(input)
            .unwrap_or_else(|error| panic!("failure should settle: {error}"));
        assert_failure(transition, kind, delivery);
        assert_eq!(machine.state(), LegacyAlterConfigsState::Completed);
    }
}

fn machine(deadline: u64) -> LegacyAlterConfigsMachine {
    LegacyAlterConfigsMachine::new(
        OperationId::from_raw(12),
        Deadline::from_tick(deadline),
        plan_fixture(),
    )
}

fn submitted_machine() -> LegacyAlterConfigsMachine {
    let mut machine = machine(20);
    machine
        .apply(LegacyAlterConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(LegacyAlterConfigsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn plan_fixture() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::new(
        vec![
            LegacyTopicConfigReplacement::new(
                "orders".to_owned(),
                vec![
                    LegacyConfigEntry::new("retention.ms".to_owned(), Some("10".to_owned())),
                    LegacyConfigEntry::new("segment.ms".to_owned(), None),
                ],
            ),
            LegacyTopicConfigReplacement::new(
                "audit".to_owned(),
                vec![LegacyConfigEntry::new(
                    "cleanup.policy".to_owned(),
                    Some("compact".to_owned()),
                )],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
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
