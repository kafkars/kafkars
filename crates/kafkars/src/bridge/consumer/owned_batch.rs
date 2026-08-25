//! Private facade transfer over lease-preserving engine-owned consumer records.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_engine::{
    AssignedConsumerHeader as EngineHeader, AssignedConsumerOwnedBatch as EngineBatch,
    AssignedConsumerOwnedRecord as EngineRecord, AssignedConsumerOwnedRecords as EngineRecords,
};

use crate::{
    header_name::SourceOwner,
    record::{Header, Record, RecordTransferParts},
};

type ReservedTransfer<T> = (T, Arc<str>, Vec<Header>);
type RejectedTransfer<T> = (T, Arc<str>);

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

    pub(crate) fn headers(
        &self,
    ) -> impl ExactSizeIterator<Item = AssignedConsumerOwnedHeader<'_>> + '_ {
        self.inner
            .headers()
            .map(|inner| AssignedConsumerOwnedHeader { inner })
    }

    pub(crate) fn try_into_record(
        self,
        target_topic: Arc<str>,
    ) -> Result<(Record, Self), (Self, Arc<str>)> {
        let header_count = self.headers().len();
        let (source, target_topic, headers) =
            reserve_transfer_headers(self, target_topic, header_count)?;
        let source_owner = SourceOwner::new(source.inner.shared_source_owner());
        let record = record_from_reserved_shared_delivery_parts(
            target_topic,
            source.timestamp_millis(),
            source.inner.shared_key(),
            source.inner.shared_value(),
            source
                .headers()
                .map(AssignedConsumerOwnedHeader::into_shared_parts),
            source_owner,
            headers,
        );
        Ok((record, source))
    }
}

pub(super) fn reserve_transfer_headers<T>(
    source: T,
    target_topic: Arc<str>,
    header_count: usize,
) -> Result<ReservedTransfer<T>, RejectedTransfer<T>> {
    let mut headers = Vec::new();
    if headers.try_reserve_exact(header_count).is_err() {
        return Err((source, target_topic));
    }
    Ok((source, target_topic, headers))
}

pub(super) fn record_from_reserved_shared_delivery_parts(
    target_topic: Arc<str>,
    timestamp: Option<i64>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    source_headers: impl ExactSizeIterator<Item = (Bytes, Option<Bytes>)>,
    source_owner: SourceOwner,
    mut headers: Vec<Header>,
) -> Record {
    debug_assert!(headers.capacity() >= source_headers.len());
    for (name, value) in source_headers {
        headers.push(Header::from_shared_parts(name, value, source_owner.clone()));
    }
    Record::from_transfer_parts(RecordTransferParts {
        topic: target_topic,
        expected_topic_uuid: None,
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

impl<'record> AssignedConsumerOwnedHeader<'record> {
    pub(crate) fn key(&self) -> &'record [u8] {
        self.inner.key()
    }

    pub(crate) fn value(&self) -> Option<&'record [u8]> {
        self.inner.value()
    }

    fn into_shared_parts(self) -> (Bytes, Option<Bytes>) {
        self.inner.into_shared_parts()
    }
}
