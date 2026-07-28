//! Bounded one-partition Admin `DeleteRecords` request construction.

use kafka_client_core::DeleteRecordsTarget;
use kafka_wire::{
    DeleteRecordsRequest,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
};

/// Why validated Admin `DeleteRecords` intent could not become a generated request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeleteRecordsRequestFailure {
    /// A remaining request timeout cannot be negative.
    NegativeTimeout {
        /// Exact invalid timeout supplied by the interpreter.
        actual: i32,
    },
}

/// Builds one destructive request for one caller target.
pub(crate) fn delete_records_request(
    target: &DeleteRecordsTarget,
    timeout_ms: i32,
) -> Result<DeleteRecordsRequest, DeleteRecordsRequestFailure> {
    if timeout_ms < 0 {
        return Err(DeleteRecordsRequestFailure::NegativeTimeout { actual: timeout_ms });
    }

    let mut partition = DeleteRecordsPartition::default();
    partition.partition_index = target.partition();
    partition.offset = target.before_offset();

    let mut topic = DeleteRecordsTopic::default();
    topic.name = target.topic().into();
    topic.partitions = vec![partition];

    let mut request = DeleteRecordsRequest::default();
    request.topics = vec![topic];
    request.timeout_ms = timeout_ms;
    Ok(request)
}
