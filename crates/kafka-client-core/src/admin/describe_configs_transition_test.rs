//! Scenarios for `DescribeConfigs` lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeConfigBrokerError, DescribeConfigEntry, DescribeConfigOutcome, DescribeConfigsBatch,
    DescribeConfigsEffect, DescribeConfigsFailureKind, DescribeConfigsInput,
    DescribeConfigsMachine, DescribeConfigsMachineError, DescribeConfigsPlan,
    DescribeConfigsResourceQuery, DescribeConfigsState, DescribeConfigsTerminal,
};

#[test]
fn ordered_terminal_retains_exact_error_and_positive_throttle_once() {
    let mut machine = machine(20);
    let started = machine
        .apply(DescribeConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(DescribeConfigsEffect::Submit { deadline, plan, .. }) = started.into_effect() else {
        panic!("start must submit");
    };
    assert_eq!(deadline, Deadline::from_tick(20));
    assert!(plan.include_synonyms());
    assert!(plan.include_documentation());
    machine
        .apply(DescribeConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = DescribeConfigsBatch::new(
        77,
        vec![
            DescribeConfigOutcome::described(
                2,
                "orders",
                vec![config("cleanup.policy"), config("retention.ms")],
            ),
            DescribeConfigOutcome::failed(
                4,
                "7",
                DescribeConfigBrokerError::new(code, Some("future error".to_owned()), false),
            ),
        ],
    );
    let terminal = machine
        .apply(DescribeConfigsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(DescribeConfigsEffect::Complete {
        terminal: DescribeConfigsTerminal::Configs(batch),
        ..
    }) = terminal.into_effect()
    else {
        panic!("response must complete");
    };
    assert_eq!(batch.throttle_time_ms(), 77);
    let super::DescribeConfigResult::Failed(error) = batch.resources()[1].result() else {
        panic!("broker resource must retain its failure");
    };
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future error"));
    assert!(!error.message_truncated());
    assert_eq!(machine.state(), DescribeConfigsState::Completed);
    assert_eq!(
        machine.apply(DescribeConfigsInput::InvalidResponse),
        Err(DescribeConfigsMachineError::AlreadyCompleted)
    );
}

#[test]
fn pre_driver_expiry_and_rejection_are_definitely_not_sent() {
    let mut expired = machine(4);
    assert_failure(
        expired
            .apply(DescribeConfigsInput::Start {
                now: Moment::from_tick(4),
            })
            .unwrap_or_else(|error| panic!("elapsed start should settle: {error}")),
        DescribeConfigsFailureKind::DeadlineElapsed,
        DeliveryStatus::NotSent,
    );

    let mut rejected = machine(20);
    rejected
        .apply(DescribeConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert_failure(
        rejected
            .apply(DescribeConfigsInput::DriverRejected)
            .unwrap_or_else(|error| panic!("driver rejection should settle: {error}")),
        DescribeConfigsFailureKind::DriverRejected,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn driver_owned_deadline_and_transport_preserve_authoritative_certainty() {
    let mut deadline = submitted_machine();
    assert_failure(
        deadline
            .apply(DescribeConfigsInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            })
            .unwrap_or_else(|error| panic!("driver deadline should settle: {error}")),
        DescribeConfigsFailureKind::DeadlineElapsed,
        DeliveryStatus::PossiblySent,
    );

    let mut transport = submitted_machine();
    assert_failure(
        transport
            .apply(DescribeConfigsInput::TransportFailed {
                delivery: DeliveryStatus::NotSent,
            })
            .unwrap_or_else(|error| panic!("transport should settle: {error}")),
        DescribeConfigsFailureKind::Transport,
        DeliveryStatus::NotSent,
    );
}

#[test]
fn invalid_too_large_and_compatibility_have_distinct_terminal_categories() {
    for (input, kind, delivery) in [
        (
            DescribeConfigsInput::InvalidResponse,
            DescribeConfigsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeConfigsInput::ResponseTooLarge,
            DescribeConfigsFailureKind::ResponseTooLarge,
            DeliveryStatus::PossiblySent,
        ),
        (
            DescribeConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            DescribeConfigsFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            DescribeConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            DescribeConfigsFailureKind::Compatibility,
            DeliveryStatus::PossiblySent,
        ),
    ] {
        let mut machine = submitted_machine();
        assert_failure(
            machine
                .apply(input)
                .unwrap_or_else(|error| panic!("failure should settle: {error}")),
            kind,
            delivery,
        );
    }
}

#[test]
fn response_resource_and_configuration_order_are_revalidated_before_terminal() {
    let mut machine = submitted_machine();
    let wrong_count = DescribeConfigsBatch::new(
        0,
        vec![DescribeConfigOutcome::described(2, "orders", Vec::new())],
    );
    assert_eq!(
        machine.apply(DescribeConfigsInput::BrokerResponded { batch: wrong_count }),
        Err(DescribeConfigsMachineError::OutcomeCountMismatch)
    );
    let wrong_resource = DescribeConfigsBatch::new(
        0,
        vec![
            DescribeConfigOutcome::described(4, "7", Vec::new()),
            DescribeConfigOutcome::described(2, "orders", Vec::new()),
        ],
    );
    assert_eq!(
        machine.apply(DescribeConfigsInput::BrokerResponded {
            batch: wrong_resource
        }),
        Err(DescribeConfigsMachineError::OutcomeResourceMismatch)
    );
    let wrong_configs = DescribeConfigsBatch::new(
        0,
        vec![
            DescribeConfigOutcome::described(
                2,
                "orders",
                vec![config("retention.ms"), config("cleanup.policy")],
            ),
            DescribeConfigOutcome::described(4, "7", Vec::new()),
        ],
    );
    assert_eq!(
        machine.apply(DescribeConfigsInput::BrokerResponded {
            batch: wrong_configs
        }),
        Err(DescribeConfigsMachineError::ConfigurationCorrelationMismatch)
    );
    assert_eq!(machine.state(), DescribeConfigsState::Submitted);

    let valid = DescribeConfigsBatch::new(
        3,
        vec![
            DescribeConfigOutcome::described(2, "orders", vec![config("retention.ms")]),
            DescribeConfigOutcome::described(4, "7", Vec::new()),
        ],
    );
    assert!(
        machine
            .apply(DescribeConfigsInput::BrokerResponded { batch: valid })
            .is_ok()
    );
}

fn machine(deadline: u64) -> DescribeConfigsMachine {
    DescribeConfigsMachine::new(
        OperationId::from_raw(12),
        Deadline::from_tick(deadline),
        plan(),
    )
}

fn submitted_machine() -> DescribeConfigsMachine {
    let mut machine = machine(20);
    machine
        .apply(DescribeConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeConfigsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("submit machine: {error}"));
    machine
}

fn plan() -> DescribeConfigsPlan {
    DescribeConfigsPlan::new(
        vec![
            DescribeConfigsResourceQuery::new(
                2,
                "orders".to_owned(),
                Some(vec!["cleanup.policy".to_owned(), "retention.ms".to_owned()]),
            ),
            DescribeConfigsResourceQuery::new(4, "7".to_owned(), None),
        ],
        true,
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn config(name: &str) -> DescribeConfigEntry {
    DescribeConfigEntry::new(
        name.to_owned(),
        None,
        false,
        0,
        false,
        Vec::new(),
        Some(0),
        None,
    )
}

fn assert_failure(
    transition: super::DescribeConfigsTransition,
    expected_kind: DescribeConfigsFailureKind,
    expected_delivery: DeliveryStatus,
) {
    let Some(DescribeConfigsEffect::Complete {
        terminal: DescribeConfigsTerminal::Failed(failure),
        ..
    }) = transition.into_effect()
    else {
        panic!("expected failed terminal");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(failure.delivery(), expected_delivery);
}
