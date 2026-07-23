//! Fixed-capacity ownership of encoded Produce requests before driver transfer.

mod release;
#[cfg(test)]
mod release_test;

use std::{collections::BTreeMap, error::Error, fmt};

use kafka_client_core::{BatchExecutionId, BatchId};

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
    /// The logical batch owns bytes from a different execution generation.
    ExecutionMismatch,
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
            Self::ExecutionMismatch => "prepared Produce execution identity is stale",
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
    batches: BTreeMap<BatchId, PreparedBatch>,
}

#[derive(Debug)]
struct PreparedBatch {
    execution: BatchExecutionId,
    value: MaterializedProduce,
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
        execution: BatchExecutionId,
        value: MaterializedProduce,
    ) -> Result<(), PreparedInsertError> {
        let batch_id = execution.batch_id();
        if let Some(current) = self.batches.get(&batch_id) {
            let reason = if current.execution == execution {
                PreparedProduceError::DuplicateBatch
            } else {
                PreparedProduceError::ExecutionMismatch
            };
            return Err(PreparedInsertError::new(reason, value));
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
        self.batches
            .insert(batch_id, PreparedBatch { execution, value });
        self.retained_bytes = next_bytes;
        Ok(())
    }

    /// Returns whether this store is the encoded-byte owner for a batch.
    pub(crate) fn contains(&self, execution: BatchExecutionId) -> bool {
        self.batches
            .get(&execution.batch_id())
            .is_some_and(|entry| entry.execution == execution)
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
}
