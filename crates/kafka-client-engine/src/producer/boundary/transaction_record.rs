//! Transactional validation and one shared materialization view of a public record.

use std::sync::Arc;

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use super::record::ProducerRecord;
use crate::producer::{
    materialization::{MaterializationHeader, MaterializationRecord},
    record::HEADER_CONTROL_BYTES,
};

/// One validated transactional route and shared materialization view.
#[derive(Debug)]
pub(crate) struct TransactionRecordView {
    topic: Arc<str>,
    partition: Option<PartitionIndex>,
    materialization: MaterializationRecord,
    source_retained_bytes: usize,
}

impl TransactionRecordView {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<str>,
        Option<PartitionIndex>,
        MaterializationRecord,
        usize,
    ) {
        (
            self.topic,
            self.partition,
            self.materialization,
            self.source_retained_bytes,
        )
    }
}

/// Local transactional-record validation or retained-size failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionRecordViewError {
    EmptyTopic,
    NegativeExplicitPartition,
    RetainedSizeOverflow,
}

impl ProducerRecord {
    pub(crate) fn transaction_view(
        &self,
        default_timestamp_ms: i64,
    ) -> Result<TransactionRecordView, TransactionRecordViewError> {
        if self.topic.is_empty() {
            return Err(TransactionRecordViewError::EmptyTopic);
        }
        let partition = match self.partition {
            None => None,
            Some(partition) if partition < 0 => {
                return Err(TransactionRecordViewError::NegativeExplicitPartition);
            }
            Some(partition) => Some(PartitionIndex::from_raw(
                u32::try_from(partition)
                    .map_err(|_error| TransactionRecordViewError::NegativeExplicitPartition)?,
            )),
        };
        let source_retained_bytes = self.transaction_retained_bytes()?;
        let headers = self
            .headers
            .iter()
            .map(|header| {
                MaterializationHeader::new(header.shared_name_bytes(), header.shared_value())
            })
            .collect();
        Ok(TransactionRecordView {
            topic: Arc::clone(&self.topic),
            partition,
            materialization: MaterializationRecord::new(
                self.timestamp_ms.unwrap_or(default_timestamp_ms),
                self.key.clone(),
                self.value.clone(),
                headers,
            ),
            source_retained_bytes,
        })
    }

    fn transaction_retained_bytes(&self) -> Result<usize, TransactionRecordViewError> {
        let fields = self
            .topic
            .len()
            .checked_add(self.key.as_ref().map_or(0, Bytes::len))
            .and_then(|size| size.checked_add(self.value.as_ref().map_or(0, Bytes::len)))
            .ok_or(TransactionRecordViewError::RetainedSizeOverflow)?;
        let fields = self.headers.iter().try_fold(fields, |size, header| {
            size.checked_add(header.name_len())
                .and_then(|size| size.checked_add(header.value().map_or(0, Bytes::len)))
                .ok_or(TransactionRecordViewError::RetainedSizeOverflow)
        })?;
        self.headers
            .capacity()
            .checked_mul(HEADER_CONTROL_BYTES)
            .and_then(|controls| fields.checked_add(controls))
            .ok_or(TransactionRecordViewError::RetainedSizeOverflow)
    }
}
