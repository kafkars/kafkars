//! Linear compression job and exact-generation completion values.

use kafka_client_core::{BatchExecutionId, CompressionPolicy};

use crate::{
    producer::{batch_store::MaterializationAttempt, materialization::MaterializationBatch},
    protocol::produce::{MaterializedProduce, materialize_explicit_produce_batch_with_compression},
};

/// One exact sealed execution transferred to a native compression worker.
#[derive(Debug)]
#[must_use = "a compression job must be submitted or returned to the store"]
pub(crate) struct CompressionJob {
    execution: BatchExecutionId,
    attempt: MaterializationAttempt,
    input: MaterializationBatch,
    compression: CompressionPolicy,
    reservation_bytes: usize,
}

impl CompressionJob {
    pub(crate) fn new(
        attempt: MaterializationAttempt,
        input: MaterializationBatch,
        compression: CompressionPolicy,
    ) -> Result<Self, MaterializationAttempt> {
        let Some(reservation_bytes) = input
            .source_retained_bytes()
            .checked_add(input.max_batch_bytes())
        else {
            return Err(attempt);
        };
        Ok(Self {
            execution: attempt.execution(),
            attempt,
            input,
            compression,
            reservation_bytes,
        })
    }

    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    pub(crate) const fn reservation_bytes(&self) -> usize {
        self.reservation_bytes
    }

    pub(crate) fn run(self) -> CompressionCompletion {
        let materialized =
            materialize_explicit_produce_batch_with_compression(self.input, self.compression).ok();
        CompressionCompletion {
            execution: self.execution,
            attempt: self.attempt,
            materialized,
        }
    }

    pub(crate) fn into_attempt(self) -> MaterializationAttempt {
        self.attempt
    }
}

/// Worker output retaining the exact store attempt until host validation.
#[derive(Debug)]
#[must_use = "a compression completion must commit or discard its exact attempt"]
pub(crate) struct CompressionCompletion {
    execution: BatchExecutionId,
    attempt: MaterializationAttempt,
    materialized: Option<MaterializedProduce>,
}

impl CompressionCompletion {
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    pub(crate) fn into_parts(self) -> (MaterializationAttempt, Option<MaterializedProduce>) {
        (self.attempt, self.materialized)
    }
}
