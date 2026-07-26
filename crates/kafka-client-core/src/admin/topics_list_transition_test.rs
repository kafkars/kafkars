//! All-topic ordering, filtering, and terminal-assignment scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, Moment, OperationId};

use super::{
    DescribeTopicBrokerError, DescribeTopicOutcome, DescribeTopicsEffect, DescribeTopicsInput,
    DescribeTopicsMachine, DescribeTopicsMachineError, DescribeTopicsPlan, DescribeTopicsTerminal,
    TopicDescription,
};

#[test]
fn all_topics_filtering_is_core_owned_and_terminal_once() {
    let mut machine = submitted_machine(false);
    let outcomes = vec![
        failed("consumer_offsets", true, -17),
        described("orders", false),
    ];
    let transition = machine
        .apply(DescribeTopicsInput::BrokerResponded { outcomes })
        .unwrap_or_else(|error| panic!("ordered all-topic facts should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Topics(outcomes),
        ..
    }) = transition.into_effect()
    else {
        panic!("topic terminal expected");
    };
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].topic(), "orders");
    assert_eq!(
        machine.apply(DescribeTopicsInput::InvalidResponse),
        Err(DescribeTopicsMachineError::AlreadyCompleted)
    );
}

#[test]
fn all_topics_can_retain_internal_successes_and_failures() {
    let mut machine = submitted_machine(true);
    let outcomes = vec![
        failed("consumer_offsets", true, -17),
        described("orders", false),
    ];
    let transition = machine
        .apply(DescribeTopicsInput::BrokerResponded { outcomes })
        .unwrap_or_else(|error| panic!("ordered all-topic facts should settle: {error}"));
    let Some(DescribeTopicsEffect::Complete {
        terminal: DescribeTopicsTerminal::Topics(outcomes),
        ..
    }) = transition.into_effect()
    else {
        panic!("topic terminal expected");
    };
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_internal());
}

#[test]
fn all_topics_rejects_empty_duplicate_and_nonlexicographic_names() {
    for (outcomes, expected) in [
        (
            vec![described("", false)],
            DescribeTopicsMachineError::EmptyOutcomeTopic,
        ),
        (
            vec![described("orders", false), failed("orders", false, -17)],
            DescribeTopicsMachineError::DuplicateOutcomeTopic,
        ),
        (
            vec![described("zeta", false), described("alpha", false)],
            DescribeTopicsMachineError::OutcomeTopicOrder,
        ),
    ] {
        let mut machine = submitted_machine(true);
        assert_eq!(
            machine.apply(DescribeTopicsInput::BrokerResponded { outcomes }),
            Err(expected)
        );
    }
}

fn submitted_machine(include_internal: bool) -> DescribeTopicsMachine {
    let mut machine = DescribeTopicsMachine::new(
        OperationId::from_raw(31),
        Deadline::from_tick(20),
        DescribeTopicsPlan::all(include_internal),
    );
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(DescribeTopicsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("machine should submit: {error}"));
    machine
}

fn described(topic: &str, internal: bool) -> DescribeTopicOutcome {
    DescribeTopicOutcome::described(TopicDescription::new(
        topic.to_owned(),
        None,
        internal,
        Vec::new(),
    ))
}

fn failed(topic: &str, internal: bool, code: i16) -> DescribeTopicOutcome {
    DescribeTopicOutcome::failed(
        topic,
        internal,
        DescribeTopicBrokerError::new(
            NonZeroI16::new(code).unwrap_or_else(|| panic!("test code is nonzero")),
        ),
    )
}
