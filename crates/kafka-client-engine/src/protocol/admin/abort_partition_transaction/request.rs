//! Exact one-marker `WriteTxnMarkers` request construction.

use kafka_client_core::AbortPartitionTransactionPlan;
use kafka_wire::{
    WriteTxnMarkersRequest,
    write_txn_markers_request::{WritableTxnMarker, WritableTxnMarkerTopic},
};

/// Builds one abort marker for the exact validated topic-partition and producer identity.
pub(crate) fn abort_partition_transaction_request(
    plan: &AbortPartitionTransactionPlan,
) -> WriteTxnMarkersRequest {
    let mut topic = WritableTxnMarkerTopic::default();
    topic.name = plan.topic().into();
    topic.partition_indexes = vec![plan.partition()];

    let mut marker = WritableTxnMarker::default();
    marker.producer_id = plan.producer_id();
    marker.producer_epoch = plan.producer_epoch();
    marker.transaction_result = false;
    marker.topics = vec![topic];
    marker.coordinator_epoch = plan.coordinator_epoch();
    marker.transaction_version = plan.transaction_version();

    let mut request = WriteTxnMarkersRequest::default();
    request.markers = vec![marker];
    request
}
