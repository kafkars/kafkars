//! Version, shape, correlation, and lossless response normalization scenarios.

use kafka_client_core::{AdminDescribeLogDirsPartition, AdminDescribeLogDirsSelection};
use kafka_wire::{
    DescribeLogDirsResponse,
    describe_log_dirs_response::{
        DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
    },
};

use super::{
    DescribeLogDirsResponseFailure, DescribeLogDirsSelectionRef,
    DescribeLogDirsSelectionResponseFailure, DescribeLogDirsTopicSelectionRef,
    normalize_describe_log_dirs_response, normalize_describe_log_dirs_response_for_selection,
};

#[test]
fn v5_preserves_selected_version_exact_codes_and_signed_replica_facts() {
    let mut response = response(vec![
        log_dir("/b", -31_999, vec![topic("zeta", vec![partition(3)])]),
        log_dir("/a", 17, vec![topic("alpha", vec![partition(1)])]),
    ]);
    response.throttle_time_ms = 91;
    response.error_code = -32_000;
    response.results[0].total_bytes = -7;
    response.results[0].usable_bytes = 42;
    response.results[0].is_cordoned = true;
    response.results[0].topics[0].partitions[0].partition_size = -9;
    response.results[0].topics[0].partitions[0].offset_lag = -11;
    response.results[0].topics[0].partitions[0].is_future_key = true;

    let normalized = normalize_describe_log_dirs_response(
        DescribeLogDirsSelectionRef::AllTopics,
        5,
        &response,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("valid v5 response: {error:?}"));
    assert_eq!(normalized.selected_version(), 5);
    assert_eq!(normalized.throttle_time_ms(), 91);
    assert_eq!(normalized.error_code(), -32_000);
    assert_eq!(normalized.log_dirs()[0].path(), "/a");
    assert_eq!(normalized.log_dirs()[1].path(), "/b");
    let directory = &normalized.log_dirs()[1];
    assert_eq!(directory.error_code(), -31_999);
    assert_eq!(directory.total_bytes(), Some(-7));
    assert_eq!(directory.usable_bytes(), Some(42));
    assert_eq!(directory.is_cordoned(), Some(true));
    let replica = &directory.topics()[0].partitions()[0];
    assert_eq!(replica.partition_index(), 3);
    assert_eq!(replica.partition_size(), -9);
    assert_eq!(replica.offset_lag(), -11);
    assert!(replica.is_future());
}

#[test]
fn selected_response_must_be_a_subset_and_is_sorted_deterministically() {
    let alpha = [1, 3];
    let zeta = [7];
    let selected = [
        DescribeLogDirsTopicSelectionRef::new("zeta", &zeta),
        DescribeLogDirsTopicSelectionRef::new("alpha", &alpha),
    ];
    let generated = response(vec![log_dir(
        "/data",
        0,
        vec![
            topic("zeta", vec![partition(7)]),
            topic("alpha", vec![partition(3), partition(1)]),
        ],
    )]);
    let normalized = normalize_describe_log_dirs_response(
        DescribeLogDirsSelectionRef::Selected(&selected),
        3,
        &generated,
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("selected subset response: {error:?}"));
    let topics = normalized.log_dirs()[0].topics();
    assert_eq!(topics[0].name(), "alpha");
    assert_eq!(topics[1].name(), "zeta");
    assert_eq!(topics[0].partitions()[0].partition_index(), 1);
    assert_eq!(topics[0].partitions()[1].partition_index(), 3);

    let unexpected = response(vec![log_dir(
        "/data",
        0,
        vec![topic("alpha", vec![partition(2)])],
    )]);
    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::Selected(&selected),
            3,
            &unexpected,
            usize::MAX,
        ),
        Err(DescribeLogDirsResponseFailure::UnexpectedPartition { actual: 2 })
    );
}

#[test]
fn flat_core_selection_is_grouped_before_exact_response_correlation() {
    let selection = AdminDescribeLogDirsSelection::Selected(vec![
        AdminDescribeLogDirsPartition::new("orders".to_owned(), 3),
        AdminDescribeLogDirsPartition::new("orders".to_owned(), 1),
    ]);
    let normalized = normalize_describe_log_dirs_response_for_selection(
        &selection,
        5,
        &response(vec![log_dir(
            "/data",
            0,
            vec![topic("orders", vec![partition(1)])],
        )]),
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("selected response: {error:?}"));
    assert_eq!(normalized.log_dirs()[0].topics()[0].name(), "orders");
    assert_eq!(
        normalized.log_dirs()[0].topics()[0].partitions()[0].partition_index(),
        1
    );

    assert_eq!(
        normalize_describe_log_dirs_response_for_selection(
            &selection,
            5,
            &response(vec![log_dir(
                "/data",
                0,
                vec![topic("orders", vec![partition(2)])],
            )]),
            usize::MAX,
        ),
        Err(DescribeLogDirsSelectionResponseFailure::Response(
            DescribeLogDirsResponseFailure::UnexpectedPartition { actual: 2 },
        ))
    );
}

#[test]
fn selected_version_rejects_unrepresentable_newer_fields() {
    let mut generated = response(vec![log_dir("/data", 0, Vec::new())]);
    generated.error_code = 9;
    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            2,
            &generated,
            usize::MAX,
        ),
        Err(DescribeLogDirsResponseFailure::UnrepresentableTopLevelError { actual: 9 })
    );

    generated.error_code = 0;
    generated.results[0].total_bytes = 1;
    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            3,
            &generated,
            usize::MAX,
        ),
        Err(DescribeLogDirsResponseFailure::UnrepresentableVolumeBytes)
    );

    generated.results[0].total_bytes = -1;
    generated.results[0].is_cordoned = true;
    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            4,
            &generated,
            usize::MAX,
        ),
        Err(DescribeLogDirsResponseFailure::UnrepresentableCordonState)
    );
}

#[test]
fn duplicate_and_malformed_shapes_never_bind() {
    let duplicate = response(vec![
        log_dir("/data", 0, Vec::new()),
        log_dir("/data", 7, Vec::new()),
    ]);
    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            5,
            &duplicate,
            usize::MAX,
        ),
        Err(DescribeLogDirsResponseFailure::DuplicateLogDir)
    );

    let negative = response(vec![log_dir(
        "/data",
        0,
        vec![topic("orders", vec![partition(-1)])],
    )]);
    assert_eq!(
        normalize_describe_log_dirs_response(
            DescribeLogDirsSelectionRef::AllTopics,
            5,
            &negative,
            usize::MAX,
        ),
        Err(DescribeLogDirsResponseFailure::NegativePartition { actual: -1 })
    );
}

fn response(results: Vec<DescribeLogDirsResult>) -> DescribeLogDirsResponse {
    let mut response = DescribeLogDirsResponse::default();
    response.results = results;
    response
}

fn log_dir(
    path: &str,
    error_code: i16,
    topics: Vec<DescribeLogDirsTopic>,
) -> DescribeLogDirsResult {
    let mut result = DescribeLogDirsResult::default();
    result.log_dir = path.into();
    result.error_code = error_code;
    result.topics = topics;
    result
}

fn topic(name: &str, partitions: Vec<DescribeLogDirsPartition>) -> DescribeLogDirsTopic {
    let mut topic = DescribeLogDirsTopic::default();
    topic.name = name.into();
    topic.partitions = partitions;
    topic
}

fn partition(partition_index: i32) -> DescribeLogDirsPartition {
    let mut partition = DescribeLogDirsPartition::default();
    partition.partition_index = partition_index;
    partition
}
