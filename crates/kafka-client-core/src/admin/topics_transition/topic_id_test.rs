//! Topic-ID selection and terminal-correlation scenarios.

use core::num::NonZeroI16;

use crate::{Deadline, Moment, OperationId};

use crate::admin::{
    DescribeTopicBrokerError, DescribeTopicIdOutcome, DescribeTopicOutcome, DescribeTopicsEffect,
    DescribeTopicsInput, DescribeTopicsMachine, DescribeTopicsMachineError, DescribeTopicsPlan,
    DescribeTopicsTerminal, TopicDescription,
};

#[test]
fn topic_id_terminal_preserves_exact_caller_identity_and_rejects_wrong_shape() {
    let first = [1; 16];
    let second = [2; 16];
    let plan = DescribeTopicsPlan::by_ids(vec![first, second])
        .unwrap_or_else(|error| panic!("valid topic-ID plan: {error}"));
    let mut machine =
        DescribeTopicsMachine::new(OperationId::from_raw(12), Deadline::from_tick(20), plan);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start topic-ID plan: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept topic-ID plan: {error}"));

    let wrong = vec![DescribeTopicIdOutcome::described(
        second,
        TopicDescription::new("audit".to_owned(), Some(second), false, Vec::new()),
    )];
    assert_eq!(
        machine.apply(DescribeTopicsInput::BrokerRespondedById { outcomes: wrong }),
        Err(DescribeTopicsMachineError::OutcomeCountMismatch)
    );

    let error = DescribeTopicBrokerError::new(
        NonZeroI16::new(3).unwrap_or_else(|| panic!("nonzero broker code")),
    );
    let outcomes = vec![
        DescribeTopicIdOutcome::described(
            first,
            TopicDescription::new("orders".to_owned(), Some(first), false, Vec::new()),
        ),
        DescribeTopicIdOutcome::failed(second, error),
    ];
    let terminal = machine
        .apply(DescribeTopicsInput::BrokerRespondedById { outcomes })
        .unwrap_or_else(|error| panic!("settle topic-ID plan: {error}"));
    assert!(matches!(
        terminal.into_effect(),
        Some(DescribeTopicsEffect::Complete {
            terminal: DescribeTopicsTerminal::TopicIds(outcomes),
            ..
        }) if outcomes.iter().map(DescribeTopicIdOutcome::topic_id).eq([first, second])
    ));
}

#[test]
fn name_terminal_cannot_settle_topic_id_selection() {
    let plan = DescribeTopicsPlan::by_ids(vec![[3; 16]])
        .unwrap_or_else(|error| panic!("valid topic-ID plan: {error}"));
    let mut machine =
        DescribeTopicsMachine::new(OperationId::from_raw(13), Deadline::from_tick(20), plan);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start topic-ID plan: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept topic-ID plan: {error}"));
    let outcomes = vec![DescribeTopicOutcome::described(TopicDescription::new(
        "orders".to_owned(),
        Some([3; 16]),
        false,
        Vec::new(),
    ))];
    assert_eq!(
        machine.apply(DescribeTopicsInput::BrokerResponded { outcomes }),
        Err(DescribeTopicsMachineError::OutcomeSelectionMismatch)
    );
}
