//! Broker error, hostile shape, scalar, count, and capacity scenarios.

use kafka_wire::describe_producers_response::{PartitionResponse, TopicResponse};

use super::{
    DescribeProducersProtocolFailure, NormalizedDescribeProducerResult,
    model::DESCRIBE_PRODUCERS_MAX_STATES,
    normalize_describe_producers_response,
    response_success_test::{normalize, producer, response, target},
};

const RETAINED_LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn broker_error_preserves_exact_code_and_utf8_safe_bounded_diagnostic() {
    let diagnostic = "é".repeat(600);
    let normalized = normalize(
        &response(0, -32_000, Some(diagnostic), Vec::new()),
        RETAINED_LIMIT,
    )
    .unwrap_or_else(|error| panic!("broker error: {error:?}"));
    let NormalizedDescribeProducerResult::BrokerFailed(error) = normalized.result() else {
        panic!("broker error became success");
    };
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.message().map(str::len), Some(1024));
    assert!(
        error
            .message()
            .is_some_and(|message| message.is_char_boundary(message.len()))
    );
    assert!(error.message_truncated());
}

#[test]
fn exact_version_throttle_and_target_correlation_are_strict() {
    let valid = response(0, 0, None, Vec::new());
    for actual in [-1, 1, i16::MAX] {
        assert_eq!(
            normalize_describe_producers_response(&target(), actual, &valid, RETAINED_LIMIT),
            Err(DescribeProducersProtocolFailure::UnsupportedApiVersion { actual })
        );
    }
    assert_eq!(
        normalize(&response(-1, 0, None, Vec::new()), RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::NegativeThrottleTime { actual: -1 })
    );

    let mut no_topics = valid.clone();
    no_topics.topics.clear();
    assert_eq!(
        normalize(&no_topics, RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::UnexpectedTopicCount { actual: 0 })
    );
    let mut wrong_topic = valid.clone();
    wrong_topic.topics[0].name = "other".into();
    assert_eq!(
        normalize(&wrong_topic, RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::UnexpectedTopic)
    );
    let mut wrong_partition = valid;
    wrong_partition.topics[0].partitions[0].partition_index = 8;
    assert_eq!(
        normalize(&wrong_partition, RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::UnexpectedPartition { actual: 8 })
    );
}

#[test]
fn success_and_error_payloads_cannot_be_ambiguously_mixed() {
    assert_eq!(
        normalize(
            &response(0, 0, Some("unexpected".to_owned()), Vec::new()),
            RETAINED_LIMIT,
        ),
        Err(DescribeProducersProtocolFailure::DiagnosticOnSuccess)
    );
    assert_eq!(
        normalize(
            &response(0, 1, None, vec![producer(1, 0, 0, 0, 0, -1)]),
            RETAINED_LIMIT,
        ),
        Err(DescribeProducersProtocolFailure::ProducerStatesWithPartitionError { actual: 1 })
    );
}

#[test]
fn producer_sentinels_and_nonnegative_identifiers_are_validated() {
    let cases = [
        (
            producer(-1, 0, 0, 0, 0, -1),
            DescribeProducersProtocolFailure::NegativeProducerId { actual: -1 },
        ),
        (
            producer(1, -1, 0, 0, 0, -1),
            DescribeProducersProtocolFailure::NegativeProducerEpoch { actual: -1 },
        ),
        (
            producer(1, 0, -2, 0, 0, -1),
            DescribeProducersProtocolFailure::InvalidLastSequence { actual: -2 },
        ),
        (
            producer(1, 0, 0, -2, 0, -1),
            DescribeProducersProtocolFailure::InvalidLastTimestamp { actual: -2 },
        ),
        (
            producer(1, 0, 0, 0, -1, -1),
            DescribeProducersProtocolFailure::NegativeCoordinatorEpoch { actual: -1 },
        ),
        (
            producer(1, 0, 0, 0, 0, -2),
            DescribeProducersProtocolFailure::InvalidCurrentTransactionStartOffset { actual: -2 },
        ),
    ];
    for (state, expected) in cases {
        assert_eq!(
            normalize(&response(0, 0, None, vec![state]), RETAINED_LIMIT),
            Err(expected)
        );
    }
}

#[test]
fn duplicate_hostile_count_and_retained_capacity_are_rejected() {
    assert_eq!(
        normalize(
            &response(
                0,
                0,
                None,
                vec![producer(7, 0, 0, 0, 0, -1), producer(7, 1, 1, 1, 1, 3),],
            ),
            RETAINED_LIMIT,
        ),
        Err(DescribeProducersProtocolFailure::DuplicateProducerId { actual: 7 })
    );

    let too_many = vec![producer(1, 0, 0, 0, 0, -1); DESCRIBE_PRODUCERS_MAX_STATES + 1];
    assert_eq!(
        normalize(&response(0, 0, None, too_many), RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::TooManyProducerStates {
            actual: DESCRIBE_PRODUCERS_MAX_STATES + 1,
            max: DESCRIBE_PRODUCERS_MAX_STATES,
        })
    );
    assert!(matches!(
        normalize(&response(0, 0, None, Vec::new()), 0),
        Err(DescribeProducersProtocolFailure::RetainedBytes {
            required,
            limit: 0,
        }) if required > 0
    ));
}

#[test]
fn extra_or_invalid_partition_shapes_never_bind() {
    let mut generated = response(0, 0, None, Vec::new());
    generated.topics[0]
        .partitions
        .push(PartitionResponse::default());
    assert_eq!(
        normalize(&generated, RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::UnexpectedPartitionCount { actual: 2 })
    );

    let mut generated = response(0, 0, None, Vec::new());
    generated.topics[0].partitions[0].partition_index = -1;
    assert_eq!(
        normalize(&generated, RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::NegativePartition { actual: -1 })
    );

    let mut generated = response(0, 0, None, Vec::new());
    generated.topics.push(TopicResponse::default());
    assert_eq!(
        normalize(&generated, RETAINED_LIMIT),
        Err(DescribeProducersProtocolFailure::UnexpectedTopicCount { actual: 2 })
    );
}
