//! Generated log-directory request construction scenarios.

use kafka_client_core::{AdminDescribeLogDirsPartition, AdminDescribeLogDirsSelection};
use kafka_wire::RetainedSize;
use kafka_wire_core::{ApiVersion, BytesMut, KafkaEncode};

use super::{
    DescribeLogDirsRequestFailure, DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef,
    describe_log_dirs_request, describe_log_dirs_request_for_selection,
    selection_request_peak_charge,
};

#[test]
fn flat_core_selection_groups_first_seen_topics_and_partition_order() {
    let selection = AdminDescribeLogDirsSelection::Selected(vec![
        AdminDescribeLogDirsPartition::new("orders".to_owned(), 3),
        AdminDescribeLogDirsPartition::new("audit".to_owned(), 0),
        AdminDescribeLogDirsPartition::new("orders".to_owned(), 1),
    ]);
    let request = describe_log_dirs_request_for_selection(&selection, usize::MAX)
        .unwrap_or_else(|error| panic!("selected request: {error:?}"));
    let topics = request.topics.unwrap_or_else(|| panic!("selected topics"));
    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].topic.as_str(), "orders");
    assert_eq!(topics[0].partitions, [3, 1]);
    assert_eq!(topics[1].topic.as_str(), "audit");
    assert_eq!(topics[1].partitions, [0]);
}

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
    let topics = request
        .topics
        .as_ref()
        .unwrap_or_else(|| panic!("selected topics"));
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
    .map_or_else(
        |error| error,
        |value| panic!("one byte short must fail: {value:?}"),
    );
    assert!(matches!(
        error,
        DescribeLogDirsRequestFailure::RetainedBytes { .. }
    ));
}

#[test]
fn flat_selection_peak_is_distinct_and_sufficient_for_grouped_request_scratch() {
    let selection = AdminDescribeLogDirsSelection::Selected(vec![
        AdminDescribeLogDirsPartition::new("orders".to_owned(), 3),
        AdminDescribeLogDirsPartition::new("audit".to_owned(), 0),
        AdminDescribeLogDirsPartition::new("orders".to_owned(), 1),
    ]);
    let peak = selection_request_peak_charge(&selection).unwrap_or_else(|| panic!("bounded peak"));
    assert!(peak > 0);
    describe_log_dirs_request_for_selection(&selection, peak)
        .unwrap_or_else(|error| panic!("charged selected request: {error:?}"));
    assert_eq!(
        selection_request_peak_charge(&AdminDescribeLogDirsSelection::AllTopics),
        Some(0)
    );
}
