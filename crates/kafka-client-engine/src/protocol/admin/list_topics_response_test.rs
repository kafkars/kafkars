//! All-topic Metadata response normalization scenarios.

use core::num::NonZeroI16;

use kafka_client_core::{DescribeTopicResult, DescribeTopicsInput, DescribeTopicsPlan};
use kafka_wire::{MetadataResponse, metadata_response::MetadataResponseTopic};

use super::describe_topics_response::{
    DescribeTopicsProtocolFailure, normalize_describe_topics_response,
};

#[test]
fn all_topics_are_byte_sorted_and_retain_internal_error_facts() {
    let mut response = MetadataResponse::default();
    response.topics = vec![
        topic("zeta", false, 0),
        topic("consumer_offsets", true, -731),
        topic("éclair", false, 0),
    ];
    let input =
        normalize_describe_topics_response(&DescribeTopicsPlan::all(false), &response, 4 << 20)
            .unwrap_or_else(|error| panic!("bounded list should normalize: {error:?}"));
    let DescribeTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("topic outcomes expected");
    };
    assert_eq!(
        outcomes
            .iter()
            .map(kafka_client_core::DescribeTopicOutcome::topic)
            .collect::<Vec<_>>(),
        ["consumer_offsets", "zeta", "éclair"]
    );
    assert!(outcomes[0].is_internal());
    let (_, internal, DescribeTopicResult::Failed(error)) = outcomes[0].clone().into_parts() else {
        panic!("internal error expected");
    };
    assert!(internal);
    assert_eq!(error.code(), -731);
}

#[test]
fn requested_authorized_operations_survive_normalization_before_internal_filtering() {
    let mut internal = topic("consumer_offsets", true, 0);
    internal.topic_authorized_operations = -1_234_567;
    let mut orders = topic("orders", false, 0);
    orders.topic_authorized_operations = 0x21;
    let mut response = MetadataResponse::default();
    response.topics = vec![orders, internal];
    let plan = DescribeTopicsPlan::all(false).with_authorized_operations(true);

    let input = normalize_describe_topics_response(&plan, &response, 4 << 20)
        .unwrap_or_else(|error| panic!("authorized list should normalize: {error:?}"));
    let DescribeTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("topic outcomes expected");
    };
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_internal());
    let (_, _, DescribeTopicResult::Described(internal)) = outcomes[0].clone().into_parts() else {
        panic!("internal description expected");
    };
    let (_, _, DescribeTopicResult::Described(orders)) = outcomes[1].clone().into_parts() else {
        panic!("orders description expected");
    };
    assert_eq!(internal.authorized_operations(), Some(-1_234_567));
    assert_eq!(orders.authorized_operations(), Some(0x21));
}

#[test]
fn unrequested_authorized_operations_remain_absent() {
    let mut response = MetadataResponse::default();
    response.topics = vec![topic("orders", false, 0)];

    let input =
        normalize_describe_topics_response(&DescribeTopicsPlan::all(true), &response, 4 << 20)
            .unwrap_or_else(|error| panic!("default list should normalize: {error:?}"));
    let DescribeTopicsInput::BrokerResponded { outcomes } = input else {
        panic!("topic outcomes expected");
    };
    let (_, _, DescribeTopicResult::Described(orders)) = outcomes[0].clone().into_parts() else {
        panic!("orders description expected");
    };
    assert_eq!(orders.authorized_operations(), None);
}

#[test]
fn unrequested_non_sentinel_authorized_operations_are_rejected() {
    let mut orders = topic("orders", false, 0);
    orders.topic_authorized_operations = 0x21;
    let mut response = MetadataResponse::default();
    response.topics = vec![orders];

    assert_eq!(
        normalize_describe_topics_response(&DescribeTopicsPlan::all(true), &response, 4 << 20,),
        Err(DescribeTopicsProtocolFailure::UnexpectedAuthorizedOperations)
    );
}

#[test]
fn all_topics_rejects_missing_empty_and_duplicate_names_before_terminal_copy() {
    let mut missing = topic("orders", false, 0);
    missing.name = None;
    for (topics, expected) in [
        (
            vec![missing],
            DescribeTopicsProtocolFailure::MissingTopicName,
        ),
        (
            vec![topic("", false, 0)],
            DescribeTopicsProtocolFailure::EmptyTopicName,
        ),
        (
            vec![topic("orders", false, 0), topic("orders", true, -1)],
            DescribeTopicsProtocolFailure::DuplicateTopic,
        ),
    ] {
        let mut response = MetadataResponse::default();
        response.topics = topics;
        assert_eq!(
            normalize_describe_topics_response(&DescribeTopicsPlan::all(true), &response, 4 << 20),
            Err(expected)
        );
    }
}

#[test]
fn all_topics_uses_the_accepted_aggregate_result_envelope() {
    let mut response = MetadataResponse::default();
    response.topics = vec![topic("orders", false, 0)];
    assert_eq!(
        normalize_describe_topics_response(&DescribeTopicsPlan::all(true), &response, 1),
        Err(DescribeTopicsProtocolFailure::RetainedBytes)
    );
}

#[test]
fn all_topics_top_level_error_remains_exact_and_whole_operation() {
    let mut response = MetadataResponse::default();
    response.error_code = -32_000;
    assert!(matches!(
        normalize_describe_topics_response(&DescribeTopicsPlan::all(true), &response, 1),
        Ok(DescribeTopicsInput::BrokerRejected { code })
            if code == NonZeroI16::new(-32_000)
                .unwrap_or_else(|| panic!("test code is nonzero"))
    ));
}

fn topic(name: &str, internal: bool, error_code: i16) -> MetadataResponseTopic {
    let mut topic = MetadataResponseTopic::default();
    topic.name = Some(name.into());
    topic.is_internal = internal;
    topic.error_code = error_code;
    topic
}
