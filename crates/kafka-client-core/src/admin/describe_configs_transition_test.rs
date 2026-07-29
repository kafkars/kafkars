//! Scenarios for `DescribeConfigs` lifecycle and terminal single assignment.

use core::num::NonZeroI16;

use crate::{Deadline, Moment, OperationId};

use super::{
    DescribeConfigBrokerError, DescribeConfigEntry, DescribeConfigOutcome, DescribeConfigsBatch,
    DescribeConfigsEffect, DescribeConfigsInput, DescribeConfigsMachine,
    DescribeConfigsMachineError, DescribeConfigsPlan, DescribeConfigsResourceQuery,
    DescribeConfigsRoute, DescribeConfigsState, DescribeConfigsTerminal,
};

#[test]
fn ordered_terminal_retains_exact_error_and_positive_throttle_once() {
    let mut machine = machine(20);
    let started = machine
        .apply(DescribeConfigsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(DescribeConfigsEffect::Submit {
        deadline,
        route,
        plan,
        ..
    }) = started.into_effect()
    else {
        panic!("start must submit");
    };
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(route, DescribeConfigsRoute::AnyBroker);
    assert_eq!(
        plan.resources()
            .iter()
            .map(DescribeConfigsResourceQuery::resource_name)
            .collect::<Vec<_>>(),
        ["orders", "audit"]
    );
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
            DescribeConfigOutcome::described(2, "audit", Vec::new()),
        ],
    );
    let next = machine
        .apply(DescribeConfigsInput::BrokerResponded { batch })
        .unwrap_or_else(|error| panic!("response should settle: {error}"));
    let Some(DescribeConfigsEffect::Submit {
        deadline,
        route,
        plan,
        ..
    }) = next.into_effect()
    else {
        panic!("first route must submit the second route");
    };
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(route, DescribeConfigsRoute::ExactBroker(7));
    assert_eq!(plan.resources().len(), 1);
    machine
        .apply(DescribeConfigsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("second driver acceptance should succeed: {error}"));
    let terminal = machine
        .apply(DescribeConfigsInput::BrokerResponded {
            batch: DescribeConfigsBatch::new(
                12,
                vec![DescribeConfigOutcome::failed(
                    4,
                    "7",
                    DescribeConfigBrokerError::new(code, Some("future error".to_owned()), false),
                )],
            ),
        })
        .unwrap_or_else(|error| panic!("second response should settle: {error}"));
    let Some(DescribeConfigsEffect::Complete {
        terminal: DescribeConfigsTerminal::Configs(batch),
        ..
    }) = terminal.into_effect()
    else {
        panic!("response must complete");
    };
    assert_eq!(batch.throttle_time_ms(), 77);
    assert_eq!(
        batch
            .resources()
            .iter()
            .map(|outcome| (outcome.resource_type(), outcome.resource_name()))
            .collect::<Vec<_>>(),
        [(2, "orders"), (4, "7"), (2, "audit")]
    );
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
            DescribeConfigOutcome::described(2, "orders", Vec::new()),
            DescribeConfigOutcome::described(4, "7", Vec::new()),
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
            DescribeConfigOutcome::described(2, "audit", Vec::new()),
        ],
    );
    assert_eq!(
        machine.apply(DescribeConfigsInput::BrokerResponded {
            batch: wrong_configs
        }),
        Err(DescribeConfigsMachineError::ConfigurationCorrelationMismatch)
    );
    assert_eq!(machine.state(), DescribeConfigsState::Submitted);
    let duplicate = DescribeConfigsBatch::new(
        0,
        vec![
            DescribeConfigOutcome::described(2, "orders", Vec::new()),
            DescribeConfigOutcome::described(2, "orders", Vec::new()),
        ],
    );
    assert_eq!(
        machine.apply(DescribeConfigsInput::BrokerResponded { batch: duplicate }),
        Err(DescribeConfigsMachineError::OutcomeResourceMismatch)
    );

    let valid = DescribeConfigsBatch::new(
        3,
        vec![
            DescribeConfigOutcome::described(2, "orders", vec![config("retention.ms")]),
            DescribeConfigOutcome::described(2, "audit", Vec::new()),
        ],
    );
    let next = machine
        .apply(DescribeConfigsInput::BrokerResponded { batch: valid })
        .unwrap_or_else(|error| panic!("valid route response: {error}"));
    assert!(matches!(
        next.into_effect(),
        Some(DescribeConfigsEffect::Submit {
            route: DescribeConfigsRoute::ExactBroker(7),
            ..
        })
    ));
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
            DescribeConfigsResourceQuery::new(2, "audit".to_owned(), None),
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
