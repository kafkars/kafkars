//! Selected-replica public-to-engine request translation coverage.

use crate::admin::TopicPartitionReplica;

use super::DescribeReplicaLogDirsAdminRequest;

#[test]
fn request_translation_preserves_caller_order_and_exact_broker_identity() {
    let request = DescribeReplicaLogDirsAdminRequest::new(vec![
        TopicPartitionReplica::new("orders", 2, 8),
        TopicPartitionReplica::new("audit", 0, 3),
    ])
    .into_engine();

    let debug = format!("{request:?}");
    let orders = debug
        .find("orders")
        .unwrap_or_else(|| panic!("orders target missing"));
    let audit = debug
        .find("audit")
        .unwrap_or_else(|| panic!("audit target missing"));
    assert!(orders < audit);
    assert!(debug.contains("broker_id: 8"));
    assert!(debug.contains("broker_id: 3"));
}
