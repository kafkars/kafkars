//! Scenarios for `DescribeTopics` lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    DescribeTopicBrokerError, DescribeTopicOutcome, DescribeTopicsEffect, DescribeTopicsInput,
    DescribeTopicsMachine, DescribeTopicsMachineError, DescribeTopicsPlan, DescribeTopicsState,
    DescribeTopicsTerminal, TopicDescription,
};

fn machine(deadline: u64) -> DescribeTopicsMachine {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned(), "audit".to_owned()])
        .unwrap_or_else(|error| panic!("valid DescribeTopics test plan: {error}"));
    DescribeTopicsMachine::new(
        OperationId::from_raw(11),
        Deadline::from_tick(deadline),
        plan,
    )
}

#[test]
fn ordered_terminal_is_single_assignment_and_lossless() {
    let mut machine = machine(20);
    let started = machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    assert!(matches!(
        started.into_effect(),
        Some(DescribeTopicsEffect::Submit { .. })
    ));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let code = NonZeroI16::new(-321).unwrap_or_else(|| panic!("code is nonzero"));
    let outcomes = vec![
        DescribeTopicOutcome::described(
            "orders",
            TopicDescription::new("orders".to_owned(), None, false, Vec::new()),
        ),
        DescribeTopicOutcome::failed("audit", DescribeTopicBrokerError::new(code)),
    ];
    let terminal = machine
        .apply(DescribeTopicsInput::BrokerResponded { outcomes })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(DescribeTopicsEffect::Complete {
            terminal: DescribeTopicsTerminal::Topics(_),
            ..
        })
    ));
    assert_eq!(machine.state(), DescribeTopicsState::Completed);
    assert_eq!(
        machine.apply(DescribeTopicsInput::InvalidResponse),
        Err(DescribeTopicsMachineError::AlreadyCompleted)
    );
}

#[test]
fn original_deadline_and_driver_ownership_choose_delivery_certainty() {
    let mut expired = machine(4);
    let terminal = expired
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(4),
        })
        .unwrap_or_else(|error| panic!("expiry should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("deadline should complete");
    };
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);

    let mut submitted = machine(10);
    submitted
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    submitted
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let terminal = submitted
        .apply(DescribeTopicsInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("transport failure should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("transport should complete");
    };
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn driver_deadline_is_timeout_policy_with_authoritative_certainty() {
    let mut machine = machine(10);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let terminal = machine
        .apply(DescribeTopicsInput::DriverDeadlineElapsed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("driver deadline should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("driver deadline should complete");
    };
    assert_eq!(
        failure.kind(),
        super::DescribeTopicsFailureKind::DeadlineElapsed
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn response_must_match_request_count_and_order() {
    let mut machine = machine(20);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let wrong = vec![DescribeTopicOutcome::described(
        "audit",
        TopicDescription::new("audit".to_owned(), None, false, Vec::new()),
    )];
    assert_eq!(
        machine.apply(DescribeTopicsInput::BrokerResponded { outcomes: wrong }),
        Err(DescribeTopicsMachineError::OutcomeCountMismatch)
    );
}

#[test]
fn top_level_unknown_broker_code_is_terminal_and_exact() {
    let mut machine = machine(20);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let code = NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("code is nonzero"));
    let terminal = machine
        .apply(DescribeTopicsInput::BrokerRejected { code })
        .unwrap_or_else(|error| panic!("broker rejection should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("broker rejection should complete");
    };
    assert!(matches!(
        failure.kind(),
        super::DescribeTopicsFailureKind::Broker(actual) if actual == code
    ));
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn valid_over_budget_response_is_actionable_and_possibly_sent() {
    let mut machine = machine(20);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let terminal = machine
        .apply(DescribeTopicsInput::ResponseTooLarge)
        .unwrap_or_else(|error| panic!("large response should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("large response should complete");
    };
    assert_eq!(
        failure.kind(),
        super::DescribeTopicsFailureKind::ResponseTooLarge
    );
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

#[test]
fn unrepresentable_read_only_policy_is_compatibility_and_not_sent() {
    let mut machine = machine(20);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    let terminal = machine
        .apply(DescribeTopicsInput::ProtocolIncompatible)
        .unwrap_or_else(|error| panic!("compatibility failure should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Failed(failure),
        ..
    }) = terminal.into_effect()
    else {
        panic!("compatibility failure should complete");
    };
    assert_eq!(
        failure.kind(),
        super::DescribeTopicsFailureKind::Compatibility
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}
