//! Fixed-capacity ownership of encoded Produce requests before driver transfer.

use std::{collections::BTreeMap, error::Error, fmt};

use kafka_client_core::BatchId;

use crate::protocol::produce::MaterializedProduce;

/// Failure at prepared-request insertion, transfer, or release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedProduceError {
    /// Every configured prepared-request slot is occupied.
    BatchCapacity,
    /// Retaining the encoded records would exceed their configured byte bound.
    EncodedByteCapacity,
    /// Encoded byte accounting cannot be represented.
    EncodedByteOverflow,
    /// The logical batch already owns a prepared request.
    DuplicateBatch,
    /// The logical batch is unknown, already taken, or already released.
    UnknownBatch,
}

impl fmt::Display for PreparedProduceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BatchCapacity => "prepared Produce request capacity is full",
            Self::EncodedByteCapacity => "prepared Produce encoded-byte capacity is full",
            Self::EncodedByteOverflow => "prepared Produce encoded-byte accounting overflowed",
            Self::DuplicateBatch => "batch already owns a prepared Produce request",
            Self::UnknownBatch => "prepared Produce batch identity is stale",
        })
    }
}

impl Error for PreparedProduceError {}

/// Insertion rejection that returns the still-owned materialized request.
#[derive(Debug)]
pub(crate) struct PreparedInsertError {
    reason: PreparedProduceError,
    value: MaterializedProduce,
}

impl PreparedInsertError {
    const fn new(reason: PreparedProduceError, value: MaterializedProduce) -> Self {
        Self { reason, value }
    }

    /// Returns the bounded insertion rejection.
    pub(crate) const fn reason(&self) -> PreparedProduceError {
        self.reason
    }

    /// Returns the exact request whose ownership never entered the store.
    pub(crate) fn into_value(self) -> MaterializedProduce {
        self.value
    }
}

impl fmt::Display for PreparedInsertError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(formatter)
    }
}

impl Error for PreparedInsertError {}

/// Current ownership of protocol-materialized requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreparedProduceStats {
    /// Number of retained prepared batches.
    pub(crate) batches: usize,
    /// Encoded record bytes retained across those batches.
    pub(crate) encoded_record_bytes: usize,
}

/// Linear owner of bounded requests awaiting synchronous driver submission.
#[derive(Debug)]
pub(crate) struct PreparedProduceStore {
    batch_capacity: usize,
    encoded_byte_capacity: usize,
    retained_bytes: usize,
    batches: BTreeMap<BatchId, MaterializedProduce>,
}

impl PreparedProduceStore {
    /// Creates an empty store with explicit count and encoded-record-byte bounds.
    pub(crate) const fn new(batch_capacity: usize, encoded_byte_capacity: usize) -> Self {
        Self {
            batch_capacity,
            encoded_byte_capacity,
            retained_bytes: 0,
            batches: BTreeMap::new(),
        }
    }

    /// Retains one materialized request or returns it unchanged on rejection.
    #[allow(
        clippy::result_large_err,
        reason = "bounded rejection returns the linear materialized request without allocating"
    )]
    pub(crate) fn insert(
        &mut self,
        batch_id: BatchId,
        value: MaterializedProduce,
    ) -> Result<(), PreparedInsertError> {
        if self.batches.contains_key(&batch_id) {
            return Err(PreparedInsertError::new(
                PreparedProduceError::DuplicateBatch,
                value,
            ));
        }
        if self.batches.len() >= self.batch_capacity {
            return Err(PreparedInsertError::new(
                PreparedProduceError::BatchCapacity,
                value,
            ));
        }
        let bytes = value.retained_record_bytes();
        let Some(next_bytes) = self.retained_bytes.checked_add(bytes) else {
            return Err(PreparedInsertError::new(
                PreparedProduceError::EncodedByteOverflow,
                value,
            ));
        };
        if next_bytes > self.encoded_byte_capacity {
            return Err(PreparedInsertError::new(
                PreparedProduceError::EncodedByteCapacity,
                value,
            ));
        }
        self.batches.insert(batch_id, value);
        self.retained_bytes = next_bytes;
        Ok(())
    }

    /// Transfers one request and its accounting out to the synchronous host.
    ///
    /// The caller becomes the sole owner and must immediately submit or drop the
    /// request. No lease remains in this store, so a later `release` for the same
    /// batch is an explicit `UnknownBatch` error rather than a second decrement.
    pub(crate) fn take(
        &mut self,
        batch_id: BatchId,
    ) -> Result<MaterializedProduce, PreparedProduceError> {
        self.remove(batch_id)
    }

    /// Drops one retained request and releases its accounting exactly once.
    pub(crate) fn release(&mut self, batch_id: BatchId) -> Result<usize, PreparedProduceError> {
        let value = self.remove(batch_id)?;
        Ok(value.retained_record_bytes())
    }

    /// Drops a retained request when present without weakening exact `release`.
    pub(crate) fn release_if_present(
        &mut self,
        batch_id: BatchId,
    ) -> Result<Option<usize>, PreparedProduceError> {
        if !self.batches.contains_key(&batch_id) {
            return Ok(None);
        }
        self.release(batch_id).map(Some)
    }

    /// Returns whether this store is the encoded-byte owner for a batch.
    pub(crate) fn contains(&self, batch_id: BatchId) -> bool {
        self.batches.contains_key(&batch_id)
    }

    /// Returns current count and byte ownership.
    pub(crate) fn stats(&self) -> PreparedProduceStats {
        PreparedProduceStats {
            batches: self.batches.len(),
            encoded_record_bytes: self.retained_bytes,
        }
    }

    /// Drops every encoded request after permanent execution loss.
    pub(crate) fn clear_terminal(&mut self) {
        self.batches.clear();
        self.retained_bytes = 0;
    }

    fn remove(&mut self, batch_id: BatchId) -> Result<MaterializedProduce, PreparedProduceError> {
        let value = self
            .batches
            .get(&batch_id)
            .ok_or(PreparedProduceError::UnknownBatch)?;
        let bytes = value.retained_record_bytes();
        let next_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(PreparedProduceError::EncodedByteOverflow)?;
        let value = self
            .batches
            .remove(&batch_id)
            .ok_or(PreparedProduceError::UnknownBatch)?;
        self.retained_bytes = next_bytes;
        Ok(value)
    }
}
