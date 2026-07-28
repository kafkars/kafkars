//! Generated log-directory request construction scenarios.

use kafka_wire::RetainedSize;
use kafka_wire_core::{ApiVersion, BytesMut, KafkaEncode};

use super::{
    DescribeLogDirsRequestFailure, DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef,
    describe_log_dirs_request,
};

#[test]
fn nullable_all_topics_and_explicit_selection_remain_distinct() {
    let all = describe_log_dirs_request(DescribeLogDirsSelectionRef::AllTopics, 0)
        .unwrap_or_else(|error| panic!("all-topics request: {error:?}"));
    assert_eq!(all.topics, None);

    let partitions = [7, 2];
    let selected = [DescribeLogDirsTopicSelectionRef::new("orders", &partitions)];
    let request =
        describe_log_dirs_request(DescribeLogDirsSelectionRef::Selected(&selected), usize::MAX)
            .unwrap_or_else(|error| panic!("selected request: {error:?}"));
    let topics = request.topics.as_ref().expect("selected topics");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic.as_str(), "orders");
    assert_eq!(topics[0].partitions, partitions);

    for version in 1..=5 {
        request
            .encode_into(&mut BytesMut::new(), ApiVersion::new(version))
            .unwrap_or_else(|error| panic!("v{version} request must encode: {error:?}"));
    }
}

#[test]
fn invalid_or_ambiguous_selected_shapes_are_rejected() {
    assert_eq!(
        describe_log_dirs_request(DescribeLogDirsSelectionRef::Selected(&[]), usize::MAX),
        Err(DescribeLogDirsRequestFailure::EmptySelection)
    );

    let empty_partitions = [DescribeLogDirsTopicSelectionRef::new("orders", &[])];
    assert_eq!(
        describe_log_dirs_request(
            DescribeLogDirsSelectionRef::Selected(&empty_partitions),
            usize::MAX,
        ),
        Err(DescribeLogDirsRequestFailure::EmptyPartitions)
    );

    let first = [1];
    let second = [2];
    let duplicate_topics = [
        DescribeLogDirsTopicSelectionRef::new("orders", &first),
        DescribeLogDirsTopicSelectionRef::new("orders", &second),
    ];
    assert_eq!(
        describe_log_dirs_request(
            DescribeLogDirsSelectionRef::Selected(&duplicate_topics),
            usize::MAX,
        ),
        Err(DescribeLogDirsRequestFailure::DuplicateTopic)
    );

    let duplicate_partitions = [1, 1];
    let duplicate_partition = [DescribeLogDirsTopicSelectionRef::new(
        "orders",
        &duplicate_partitions,
    )];
    assert_eq!(
        describe_log_dirs_request(
            DescribeLogDirsSelectionRef::Selected(&duplicate_partition),
            usize::MAX,
        ),
        Err(DescribeLogDirsRequestFailure::DuplicatePartition { actual: 1 })
    );
}

#[test]
fn generated_request_must_fit_before_it_is_returned() {
    let partitions = [1, 2];
    let selected = [DescribeLogDirsTopicSelectionRef::new("orders", &partitions)];
    let request =
        describe_log_dirs_request(DescribeLogDirsSelectionRef::Selected(&selected), usize::MAX)
            .unwrap_or_else(|error| panic!("unbounded request: {error:?}"));
    let exact_generated = request.retained_size().heap_bytes();

    let error = describe_log_dirs_request(
        DescribeLogDirsSelectionRef::Selected(&selected),
        exact_generated.saturating_sub(1),
    )
    .expect_err("one byte short must fail");
    assert!(matches!(
        error,
        DescribeLogDirsRequestFailure::RetainedBytes { .. }
    ));
}
