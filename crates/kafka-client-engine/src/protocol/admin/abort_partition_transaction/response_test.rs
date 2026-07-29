//! Correlation tests for one partition transaction-abort response.

use core::num::NonZeroI16;

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_wire::{
    WriteTxnMarkersResponse,
    write_txn_markers_response::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    },
};

use super::{
    AbortPartitionTransactionResponseFailure, normalize_abort_partition_transaction_response,
};

fn plan() -> AbortPartitionTransactionPlan {
    AbortPartitionTransactionPlan::new("orders".to_owned(), 3, 41, 7, 11).expect("valid abort plan")
}

fn response(error_code: i16) -> WriteTxnMarkersResponse {
    let mut partition = WritableTxnMarkerPartitionResult::default();
    partition.partition_index = 3;
    partition.error_code = error_code;

    let mut topic = WritableTxnMarkerTopicResult::default();
    topic.name = "orders".into();
    topic.partitions = vec![partition];

    let mut marker = WritableTxnMarkerResult::default();
    marker.producer_id = 41;
    marker.topics = vec![topic];

    let mut response = WriteTxnMarkersResponse::default();
    response.markers = vec![marker];
    response
}

#[test]
fn preserves_success_and_exact_signed_error() {
    assert_eq!(
        normalize_abort_partition_transaction_response(&plan(), 1, &response(0)),
        Ok(None)
    );
    assert_eq!(
        normalize_abort_partition_transaction_response(&plan(), 2, &response(-73)),
        Ok(NonZeroI16::new(-73))
    );
}

#[test]
fn rejects_mismatched_or_duplicate_identity() {
    let mut unexpected = response(0);
    unexpected.markers[0].topics[0].partitions[0].partition_index = 4;
    assert_eq!(
        normalize_abort_partition_transaction_response(&plan(), 1, &unexpected),
        Err(AbortPartitionTransactionResponseFailure::UnexpectedPartition { actual: 4 })
    );

    let mut duplicate = response(0);
    duplicate.markers.push(duplicate.markers[0].clone());
    assert_eq!(
        normalize_abort_partition_transaction_response(&plan(), 1, &duplicate),
        Err(AbortPartitionTransactionResponseFailure::DuplicateProducer)
    );
}

#[test]
fn rejects_missing_and_unsupported_version() {
    assert_eq!(
        normalize_abort_partition_transaction_response(
            &plan(),
            0,
            &WriteTxnMarkersResponse::default(),
        ),
        Err(AbortPartitionTransactionResponseFailure::UnsupportedApiVersion { actual: 0 })
    );
    assert_eq!(
        normalize_abort_partition_transaction_response(
            &plan(),
            1,
            &WriteTxnMarkersResponse::default(),
        ),
        Err(AbortPartitionTransactionResponseFailure::MissingProducer)
    );
}
