//! Timeout-free encoded bytes and late-bound generated Produce requests.

mod identity;

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::{Moment, partitioning::TopicMetadataGeneration};
use kafka_wire::{
    ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
};

use crate::clock::OperationDeadline;

pub(super) const ACKS_ALL: i16 = -1;
const NANOSECONDS_PER_MILLISECOND: u64 = 1_000_000;

/// Opaque route and separately bounded host-owned encoded batch bytes.
///
/// `ProducerStore` continues accounting for accepted application payloads until
/// core emits their release effects. The host must reserve and retain this
/// encoded batch independently until driver settlement; this type deliberately
/// contains no deadline-derived request timeout.
#[derive(Debug)]
pub(crate) struct MaterializedProduce {
    topic: Arc<str>,
    expected_topic_uuid: Option<[u8; 16]>,
    validated_topic_generation: Option<TopicMetadataGeneration>,
    partition: i32,
    record_count: u32,
    records: Bytes,
}

impl MaterializedProduce {
    pub(super) const fn new(
        topic: Arc<str>,
        partition: i32,
        record_count: u32,
        records: Bytes,
    ) -> Self {
        Self {
            topic,
            expected_topic_uuid: None,
            validated_topic_generation: None,
            partition,
            record_count,
            records,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_encoded_test_parts(
        topic: impl Into<Arc<str>>,
        partition: i32,
        records: Bytes,
    ) -> Self {
        Self::new(topic.into(), partition, 1, records)
    }

    #[cfg(test)]
    pub(crate) fn from_broker_routed_test_parts(
        topic: impl Into<Arc<str>>,
        partition: i32,
        _leader_broker_id: i32,
        records: Bytes,
    ) -> Self {
        Self::new(topic.into(), partition, 1, records)
    }

    /// Borrows the topic needed for name-routed driver admission.
    #[cfg(test)]
    pub(crate) fn topic_name(&self) -> &str {
        self.topic.as_ref()
    }

    /// Clones the existing interned owner for terminal response correlation.
    pub(crate) fn topic_owner(&self) -> Arc<str> {
        Arc::clone(&self.topic)
    }

    /// Returns the explicit partition needed for driver routing.
    pub(crate) const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns records encoded into this one partition batch.
    pub(crate) const fn record_count(&self) -> u32 {
        self.record_count
    }

    /// Returns the retained `RecordBatch` bytes awaiting driver submission.
    pub(crate) fn retained_record_bytes(&self) -> usize {
        self.records.len()
    }

    /// Consumes encoded bytes into one name-routed request at submission time.
    ///
    /// Kafka receives a rounded view of the remaining core budget. The caller
    /// retains the same copied deadline for exact transport settlement.
    pub(crate) fn into_name_routed_request(
        self,
        now: Moment,
        deadline: OperationDeadline,
    ) -> ProduceRequest {
        self.into_request(None, now, deadline)
    }

    /// Combines already materialized batches whose metadata selected one broker.
    #[cfg(test)]
    pub(crate) fn into_broker_routed_request(
        batches: Vec<Self>,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<ProduceRequest, Vec<Self>> {
        super::request_broker::build_broker_routed_request(batches, now, deadline)
    }

    /// Builds one transactional attempt while retaining the exact encoded owner.
    ///
    /// `Bytes` cloning shares the already materialized allocation. This lets the
    /// transactional send owner resubmit byte-identical `RecordBatch` bytes after
    /// a core-authorized route replacement without rematerializing records.
    pub(crate) fn transactional_name_routed_request(
        &self,
        transactional_id: &str,
        now: Moment,
        deadline: OperationDeadline,
    ) -> ProduceRequest {
        self.request(Some(transactional_id), now, deadline)
    }

    fn into_request(
        self,
        transactional_id: Option<&str>,
        now: Moment,
        deadline: OperationDeadline,
    ) -> ProduceRequest {
        Self::build_request(
            self.topic.as_ref(),
            self.partition,
            self.records,
            transactional_id,
            now,
            deadline,
        )
    }

    #[cfg(test)]
    pub(super) fn into_partition_data(self) -> (Arc<str>, PartitionProduceData) {
        let mut partition = PartitionProduceData::default();
        partition.index = self.partition;
        partition.records = Some(self.records);
        (self.topic, partition)
    }

    fn request(
        &self,
        transactional_id: Option<&str>,
        now: Moment,
        deadline: OperationDeadline,
    ) -> ProduceRequest {
        Self::build_request(
            self.topic.as_ref(),
            self.partition,
            self.records.clone(),
            transactional_id,
            now,
            deadline,
        )
    }

    fn build_request(
        topic_name: &str,
        partition_index: i32,
        records: Bytes,
        transactional_id: Option<&str>,
        now: Moment,
        deadline: OperationDeadline,
    ) -> ProduceRequest {
        let mut partition = PartitionProduceData::default();
        partition.index = partition_index;
        partition.records = Some(records);

        let mut topic = TopicProduceData::default();
        topic.name = topic_name.into();
        topic.partition_data.push(partition);

        let mut request = ProduceRequest::default();
        request.transactional_id = transactional_id.map(Into::into);
        request.acks = ACKS_ALL;
        request.timeout_ms = remaining_broker_timeout_ms(now, deadline);
        request.topic_data.push(topic);
        request
    }

    #[cfg(test)]
    pub(crate) const fn encoded_records(&self) -> &Bytes {
        &self.records
    }
}

pub(super) fn remaining_broker_timeout_ms(now: Moment, deadline: OperationDeadline) -> i32 {
    let remaining_nanoseconds = deadline.core().tick().saturating_sub(now.tick());
    let rounded_milliseconds = remaining_nanoseconds
        .saturating_add(NANOSECONDS_PER_MILLISECOND - 1)
        / NANOSECONDS_PER_MILLISECOND;
    match i32::try_from(rounded_milliseconds) {
        Ok(timeout_ms) => timeout_ms,
        Err(_overflow) => i32::MAX,
    }
}
