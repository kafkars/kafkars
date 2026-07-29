//! Lossless core-to-engine topic-terminal translation scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DescribeTopicBrokerError, DescribeTopicIdOutcome, DescribeTopicOutcome,
    DescribeTopicsEffect, DescribeTopicsInput, DescribeTopicsMachine, DescribeTopicsPlan,
    DescribeTopicsTerminal, Moment, OperationId, TopicDescription as CoreTopicDescription,
    TopicPartitionDescription as CorePartitionDescription,
};

use super::{DescribeTopicsFailureKind, DescribeTopicsOutcome, translate_terminal};

#[test]
fn topic_and_partition_codes_cross_the_engine_boundary_exactly() {
    let partition_code =
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("partition code is nonzero"));
    let topic_code = NonZeroI16::new(73).unwrap_or_else(|| panic!("topic code is nonzero"));
    let terminal = DescribeTopicsTerminal::Topics(vec![
        DescribeTopicOutcome::described(CoreTopicDescription::new(
            "orders".to_owned(),
            Some([9; 16]),
            false,
            vec![CorePartitionDescription::new(
                3,
                Some(partition_code),
                Some(7),
                Some(11),
                vec![7, 8],
                vec![7],
                vec![8],
            )],
        )),
        DescribeTopicOutcome::failed("audit", true, DescribeTopicBrokerError::new(topic_code)),
    ]);
    let DescribeTopicsOutcome::Topics(topics) = translate_terminal(terminal) else {
        panic!("topic terminal expected");
    };
    let (_, orders_internal, orders) = topics[0].clone().into_parts();
    assert!(!orders_internal);
    let description = orders.unwrap_or_else(|error| panic!("description expected: {error:?}"));
    let (_, topic_id, _, partitions, authorized_operations) = description.into_parts();
    assert_eq!(topic_id, Some([9; 16]));
    assert_eq!(authorized_operations, None);
    let (_, error, _, _, replicas, isr, offline) = partitions
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("partition expected"))
        .into_parts();
    assert_eq!(error, Some(-32_000));
    assert_eq!(replicas, [7, 8]);
    assert_eq!(isr, [7]);
    assert_eq!(offline, [8]);
    let (_, audit_internal, audit) = topics[1].clone().into_parts();
    assert!(audit_internal);
    assert_eq!(audit.err().map(super::DescribeTopicError::code), Some(73));
}

#[test]
fn internal_status_crosses_success_and_failure_translation() {
    let code = NonZeroI16::new(-731).unwrap_or_else(|| panic!("topic code is nonzero"));
    let terminal = DescribeTopicsTerminal::Topics(vec![
        DescribeTopicOutcome::described(CoreTopicDescription::new(
            "consumer_offsets".to_owned(),
            None,
            true,
            Vec::new(),
        )),
        DescribeTopicOutcome::failed("audit", false, DescribeTopicBrokerError::new(code)),
    ]);
    let DescribeTopicsOutcome::Topics(topics) = translate_terminal(terminal) else {
        panic!("topic terminal expected");
    };
    let (_, internal_success, _) = topics[0].clone().into_parts();
    let (_, internal_failure, failure) = topics[1].clone().into_parts();
    assert!(internal_success);
    assert!(!internal_failure);
    assert_eq!(
        failure.err().map(super::DescribeTopicError::code),
        Some(-731)
    );
}

#[test]
fn top_level_unknown_code_remains_a_whole_operation_failure() {
    let plan = DescribeTopicsPlan::new(vec!["orders".to_owned()])
        .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let mut machine =
        DescribeTopicsMachine::new(OperationId::from_raw(1), Deadline::from_tick(10), plan);
    machine
        .apply(DescribeTopicsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    machine
        .apply(DescribeTopicsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver: {error}"));
    let code = NonZeroI16::new(-31_999).unwrap_or_else(|| panic!("code is nonzero"));
    let transition = machine
        .apply(DescribeTopicsInput::BrokerRejected { code })
        .unwrap_or_else(|error| panic!("settle machine: {error}"));
    let Some(DescribeTopicsEffect::Complete { terminal, .. }) = transition.into_effect() else {
        panic!("terminal effect expected");
    };
    let DescribeTopicsOutcome::Failed(failure) = translate_terminal(terminal) else {
        panic!("whole failure expected");
    };
    assert_eq!(failure.kind(), DescribeTopicsFailureKind::Broker(-31_999));
}

#[test]
fn topic_id_keys_cross_the_engine_boundary_without_wire_types() {
    let topic_id = [7; 16];
    let terminal = DescribeTopicsTerminal::TopicIds(vec![DescribeTopicIdOutcome::described(
        topic_id,
        CoreTopicDescription::new("orders".to_owned(), Some(topic_id), false, Vec::new()),
    )]);
    let DescribeTopicsOutcome::TopicIds(topics) = translate_terminal(terminal) else {
        panic!("topic-ID terminal expected");
    };
    let (actual, result) = topics
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("topic-ID result expected"))
        .into_parts();
    assert_eq!(actual, topic_id);
    assert!(result.is_ok());
}
