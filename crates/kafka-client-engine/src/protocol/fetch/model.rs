//! Engine-owned descriptors retaining decoded record bytes without wire DTOs.

use bytes::Bytes;

/// One normalized generated Fetch response in broker order.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchResponse {
    pub(crate) throttle_time_ms: u32,
    pub(crate) error_code: i16,
    pub(crate) session_id: i32,
    pub(crate) topics: Vec<FetchTopic>,
    pub(crate) endpoints: Vec<FetchEndpoint>,
}

/// One bounded record payload decoded independently of ordinary Fetch metadata.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchRecordPayload {
    pub(crate) batches: Vec<FetchBatch>,
    pub(crate) records: usize,
    pub(crate) logical_bytes: usize,
}

/// One topic result retaining its name bytes without generated protocol types.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchTopic {
    pub(crate) name: Bytes,
    pub(crate) topic_id: [u8; 16],
    pub(crate) partitions: Vec<FetchPartition>,
}

/// One partition result and all broker facts required by later interpretation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchPartition {
    pub(crate) index: u32,
    pub(crate) error_code: i16,
    pub(crate) high_watermark: Option<i64>,
    pub(crate) last_stable_offset: Option<i64>,
    pub(crate) log_start_offset: Option<i64>,
    pub(crate) diverging_epoch: Option<FetchEpochEndOffset>,
    pub(crate) current_leader: Option<FetchLeader>,
    pub(crate) snapshot_id: Option<FetchEpochEndOffset>,
    pub(crate) preferred_read_replica: Option<i32>,
    pub(crate) aborted_transactions: Vec<FetchAbortedTransaction>,
    pub(crate) batches: Vec<FetchBatch>,
}

/// A broker epoch and its exclusive ending offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchEpochEndOffset {
    pub(crate) epoch: i32,
    pub(crate) end_offset: i64,
}

/// The current partition leader fact carried by a modern Fetch response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchLeader {
    pub(crate) broker_id: i32,
    pub(crate) epoch: i32,
}

/// One aborted transaction marker required by read-committed delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchAbortedTransaction {
    pub(crate) producer_id: i64,
    pub(crate) first_offset: i64,
}

/// One leader endpoint carried with a Fetch response.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchEndpoint {
    pub(crate) node_id: i32,
    pub(crate) host: Bytes,
    pub(crate) port: u16,
    pub(crate) rack: Option<Bytes>,
}

/// One retained Kafka record batch.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchBatch {
    pub(crate) base_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) next_offset: i64,
    pub(crate) partition_leader_epoch: Option<i32>,
    pub(crate) timestamp_type: FetchTimestampType,
    pub(crate) max_timestamp: Option<i64>,
    pub(crate) producer: Option<FetchProducerIdentity>,
    pub(crate) is_transactional: bool,
    pub(crate) is_control: bool,
    pub(crate) delete_horizon_ms: Option<i64>,
    pub(crate) records: Vec<FetchRecord>,
}

/// One coherent idempotent producer tuple retained with a record batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchProducerIdentity {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) base_sequence: i32,
}

/// Whether a decoded timestamp was producer- or broker-assigned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchTimestampType {
    Create,
    LogAppend,
}

/// One retained record with absolute log coordinates.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchRecord {
    pub(crate) attributes: i8,
    pub(crate) offset: i64,
    pub(crate) timestamp: Option<i64>,
    pub(crate) key: Option<Bytes>,
    pub(crate) value: Option<Bytes>,
    pub(crate) headers: Vec<FetchHeader>,
}

/// One duplicate-preserving, ordered Kafka record header.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct FetchHeader {
    pub(crate) key: Bytes,
    pub(crate) value: Option<Bytes>,
}
