//! Private facade transfer over lease-preserving engine-owned consumer records.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_engine::{
    AssignedConsumerHeader as EngineHeader, AssignedConsumerOwnedBatch as EngineBatch,
    AssignedConsumerOwnedHeader as EngineOwnedHeader, AssignedConsumerOwnedRecord as EngineRecord,
    AssignedConsumerOwnedRecords as EngineRecords,
};

use crate::{
    header_name::SourceOwner,
    record::{Header, Record, RecordTransferParts},
};

/// Private linear owned batch retaining one exact engine delivery lease.
#[derive(Debug)]
pub(crate) struct AssignedConsumerOwnedBatch {
    inner: EngineBatch,
}

impl AssignedConsumerOwnedBatch {
    pub(super) const fn from_engine(inner: EngineBatch) -> Self {
        Self { inner }
    }

    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub(crate) fn checkpoint_next_offset(&self) -> i64 {
        self.inner.checkpoint_next_offset()
    }

    pub(crate) fn record_count(&self) -> usize {
        self.inner.record_count()
    }

    pub(crate) fn into_records(self) -> AssignedConsumerOwnedRecords {
        AssignedConsumerOwnedRecords::from_engine(self.inner.into_records())
    }
}

/// Private consuming iterator retaining one shared engine delivery owner.
#[derive(Debug)]
pub(crate) struct AssignedConsumerOwnedRecords {
    inner: EngineRecords,
}

impl AssignedConsumerOwnedRecords {
    pub(super) const fn from_engine(inner: EngineRecords) -> Self {
        Self { inner }
    }
}

impl Iterator for AssignedConsumerOwnedRecords {
    type Item = AssignedConsumerOwnedRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|inner| AssignedConsumerOwnedRecord { inner })
    }
}

/// Private non-clone record translation retaining its source delivery lease.
#[derive(Debug)]
pub(crate) struct AssignedConsumerOwnedRecord {
    inner: EngineRecord,
}

impl AssignedConsumerOwnedRecord {
    pub(crate) fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub(crate) fn partition(&self) -> i32 {
        self.inner.partition()
    }

    pub(crate) fn offset(&self) -> i64 {
        self.inner.offset()
    }

    pub(crate) fn timestamp_millis(&self) -> Option<i64> {
        self.inner.timestamp_millis()
    }

    pub(crate) fn key(&self) -> Option<&[u8]> {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }

    pub(crate) fn headers(&self) -> impl ExactSizeIterator<Item = AssignedConsumerOwnedHeader<'_>> {
        self.inner
            .headers()
            .map(|inner| AssignedConsumerOwnedHeader { inner })
    }

    pub(crate) fn into_record(self, target_topic: Arc<str>) -> Record {
        let parts = self.inner.into_shared_parts();
        record_from_shared_delivery_parts(
            target_topic,
            parts.timestamp_millis,
            parts.key,
            parts.value,
            parts
                .headers
                .into_iter()
                .map(EngineOwnedHeader::into_shared_parts),
            parts.source_owner,
        )
    }
}

pub(super) fn record_from_shared_delivery_parts(
    target_topic: Arc<str>,
    timestamp: Option<i64>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: impl IntoIterator<Item = (Bytes, Option<Bytes>)>,
    source_owner: Arc<dyn Send + Sync>,
) -> Record {
    let source_owner = SourceOwner::new(source_owner);
    let headers = headers
        .into_iter()
        .map(|(name, value)| Header::from_shared_parts(name, value, source_owner.clone()))
        .collect();
    Record::from_transfer_parts(RecordTransferParts {
        topic: target_topic,
        partition: None,
        timestamp_milliseconds: timestamp,
        key,
        value,
        headers,
        source_owner,
    })
}

/// Private borrowed header view over an owned record.
#[derive(Debug)]
pub(crate) struct AssignedConsumerOwnedHeader<'record> {
    inner: EngineHeader<'record>,
}

impl AssignedConsumerOwnedHeader<'_> {
    pub(crate) fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}
