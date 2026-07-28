//! Bounded one-partition Admin `ListOffsets` request construction.

use kafka_client_core::{AdminListOffsetSpec, AdminListOffsetTarget, ReadIsolation};
use kafka_wire::{
    ListOffsetsRequest,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
};

const CONSUMER_REPLICA_ID: i32 = -1;
const READ_UNCOMMITTED: i8 = 0;
const READ_COMMITTED: i8 = 1;
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;
const MAX_TIMESTAMP: i64 = -3;
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;
const LATEST_TIERED_TIMESTAMP: i64 = -5;
const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

/// Why validated Admin `ListOffsets` intent could not become a generated request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminListOffsetsRequestFailure {
    /// A remaining request timeout cannot be negative.
    NegativeTimeout {
        /// Exact invalid timeout supplied by the interpreter.
        actual: i32,
    },
}

/// Builds one ordinary-consumer query for one caller target.
pub(crate) fn admin_list_offsets_request(
    target: &AdminListOffsetTarget,
    read_isolation: ReadIsolation,
    timeout_ms: i32,
) -> Result<ListOffsetsRequest, AdminListOffsetsRequestFailure> {
    if timeout_ms < 0 {
        return Err(AdminListOffsetsRequestFailure::NegativeTimeout { actual: timeout_ms });
    }
    let timestamp = match target.spec() {
        AdminListOffsetSpec::Earliest => EARLIEST_TIMESTAMP,
        AdminListOffsetSpec::Latest => LATEST_TIMESTAMP,
        AdminListOffsetSpec::MaxTimestamp => MAX_TIMESTAMP,
        AdminListOffsetSpec::EarliestLocal => EARLIEST_LOCAL_TIMESTAMP,
        AdminListOffsetSpec::LatestTiered => LATEST_TIERED_TIMESTAMP,
        AdminListOffsetSpec::EarliestPendingUpload => EARLIEST_PENDING_UPLOAD_TIMESTAMP,
        AdminListOffsetSpec::Timestamp(timestamp) => timestamp,
    };

    let mut partition = ListOffsetsPartition::default();
    partition.partition_index = target.partition();
    partition.current_leader_epoch = target.current_leader_epoch().unwrap_or(-1);
    partition.timestamp = timestamp;

    let mut topic = ListOffsetsTopic::default();
    topic.name = target.topic().into();
    topic.partitions = vec![partition];

    let mut request = ListOffsetsRequest::default();
    request.replica_id = CONSUMER_REPLICA_ID;
    request.isolation_level = match read_isolation {
        ReadIsolation::ReadUncommitted => READ_UNCOMMITTED,
        ReadIsolation::ReadCommitted => READ_COMMITTED,
    };
    request.topics = vec![topic];
    request.timeout_ms = timeout_ms;
    Ok(request)
}
