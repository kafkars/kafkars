//! Public homogeneous transactional batch metadata accessors.

use super::TransactionBatchMetadata;

#[test]
fn metadata_preserves_exact_batch_span_and_broker_facts() {
    let metadata =
        TransactionBatchMetadata::from_parts("orders".to_owned(), 2, 41, 43, 3, Some(55), Some(7));

    assert_eq!(metadata.topic(), "orders");
    assert_eq!(metadata.partition(), 2);
    assert_eq!(metadata.base_offset(), 41);
    assert_eq!(metadata.last_offset(), 43);
    assert_eq!(metadata.record_count(), 3);
    assert_eq!(metadata.timestamp(), Some(55));
    assert_eq!(metadata.leader_epoch(), Some(7));
}
