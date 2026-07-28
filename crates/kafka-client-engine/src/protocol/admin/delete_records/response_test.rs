//! Response correlation scenarios for Admin `DeleteRecords`.

use kafka_client_core::{DeleteRecordsResult, DeleteRecordsTarget};
use kafka_wire::{
    DeleteRecordsResponse,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
};

use super::{DeleteRecordsResponseFailure, normalize_delete_records_response};

#[test]
fn response_preserves_low_watermark_and_throttle() {
    let target = DeleteRecordsTarget::new("orders".to_owned(), 2, 91);
    let normalized = normalize_delete_records_response(&target, 2, &response(17, 2, 42, 0))
        .unwrap_or_else(|error| panic!("response must normalize: {error:?}"));

    assert_eq!(normalized.throttle_time_ms(), 17);
    let DeleteRecordsResult::Deleted(value) = normalized.outcome().result() else {
        panic!("expected deletion");
    };
    assert_eq!(value.low_watermark(), 42);
}

#[test]
fn response_preserves_exact_partition_error_and_rejects_bad_shape() {
    let target = DeleteRecordsTarget::new("orders".to_owned(), 2, 91);
    let normalized = normalize_delete_records_response(&target, 0, &response(0, 2, -1, -31_999))
        .unwrap_or_else(|error| panic!("broker error must normalize: {error:?}"));
    let DeleteRecordsResult::Failed(error) = normalized.outcome().result() else {
        panic!("expected broker failure");
    };
    assert_eq!(error.code(), -31_999);

    assert_eq!(
        normalize_delete_records_response(&target, 2, &response(0, 3, 42, 0)),
        Err(DeleteRecordsResponseFailure::UnexpectedPartition { actual: 3 })
    );
    assert_eq!(
        normalize_delete_records_response(&target, 2, &response(0, 2, -1, 0)),
        Err(DeleteRecordsResponseFailure::InvalidLowWatermark { actual: -1 })
    );
}

fn response(
    throttle_time_ms: i32,
    partition: i32,
    low_watermark: i64,
    error_code: i16,
) -> DeleteRecordsResponse {
    let mut partition_result = DeleteRecordsPartitionResult::default();
    partition_result.partition_index = partition;
    partition_result.low_watermark = low_watermark;
    partition_result.error_code = error_code;
    let mut topic = DeleteRecordsTopicResult::default();
    topic.name = "orders".into();
    topic.partitions = vec![partition_result];
    let mut response = DeleteRecordsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.topics = vec![topic];
    response
}
