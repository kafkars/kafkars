//! Request-shape tests for one partition transaction abort.

use kafka_client_core::AbortPartitionTransactionPlan;

use super::abort_partition_transaction_request;

#[test]
fn request_is_one_abort_marker_with_legacy_transaction_version() {
    let plan = AbortPartitionTransactionPlan::new("orders".to_owned(), 3, 41, 7, 11)
        .expect("valid abort plan");

    let request = abort_partition_transaction_request(&plan);

    assert_eq!(request.markers.len(), 1);
    let marker = &request.markers[0];
    assert_eq!(marker.producer_id, 41);
    assert_eq!(marker.producer_epoch, 7);
    assert!(!marker.transaction_result);
    assert_eq!(marker.coordinator_epoch, 11);
    assert_eq!(marker.transaction_version, 0);
    assert_eq!(marker.topics.len(), 1);
    assert_eq!(marker.topics[0].name.as_str(), "orders");
    assert_eq!(marker.topics[0].partition_indexes, [3]);
}
