//! Exact cancellation revision disagreements detected before publication.

use std::{error::Error, fmt};

use kafka_client_core::BatchExecutionId;

/// Exact pending or core-effect disagreement around a sealed revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerRevisionError {
    DuplicatePendingExecution(BatchExecutionId),
    OpenBatchPendingSubmit(BatchExecutionId),
    StalePendingExecution {
        expected: BatchExecutionId,
        retained: BatchExecutionId,
    },
    MissingRevisionEffect(BatchExecutionId),
    UnexpectedRevisionEffect(BatchExecutionId),
    RevisionEffectMismatch {
        expected: BatchExecutionId,
        retained: BatchExecutionId,
    },
    RevisionOutcomeMismatch(BatchExecutionId),
}

impl fmt::Display for ProducerRevisionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicatePendingExecution(execution) => write!(
                formatter,
                "batch {} generation {} has duplicate pending execution effects",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::OpenBatchPendingSubmit(execution) => write!(
                formatter,
                "open batch {} generation {} retained a pending driver submission",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::StalePendingExecution { expected, retained } => write!(
                formatter,
                "batch {} expected generation {} but retained pending generation {}",
                expected.batch_id().get(),
                expected.generation().get(),
                retained.generation().get()
            ),
            Self::MissingRevisionEffect(execution) => write!(
                formatter,
                "batch {} generation {} cancellation omitted its revision effect",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::UnexpectedRevisionEffect(execution) => write!(
                formatter,
                "batch {} generation {} cancellation lacked a preflighted revision",
                execution.batch_id().get(),
                execution.generation().get()
            ),
            Self::RevisionEffectMismatch { expected, retained } => write!(
                formatter,
                "batch {} preflighted generation {} but core revised generation {}",
                expected.batch_id().get(),
                expected.generation().get(),
                retained.generation().get()
            ),
            Self::RevisionOutcomeMismatch(execution) => write!(
                formatter,
                "batch {} generation {} revision did not produce cancelled-not-sent",
                execution.batch_id().get(),
                execution.generation().get()
            ),
        }
    }
}

impl Error for ProducerRevisionError {}
