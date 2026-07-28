//! Explicit configuration, rejection, and post-acceptance invariant failures.

mod display;

use std::{error::Error, fmt};

use kafka_client_core::{AdmissionRejection, ProducerMachineError};

use crate::{clock::BatchTimerError, completion::CompletionRegistryError};

use super::{
    ProducerStoreError, binding::OperationBindingError, cancellation::ProducerRevisionError,
    execution::PreparedExecutionError, flush::FlushBindingError, reclaim::CompletionReclaimError,
};

/// Invalid synchronization between core and engine capacity owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerHostLimitError {
    ZeroRetainedBytes,
    ZeroCompletionCapacity,
    ZeroWaitingRecordCapacity,
    ZeroWaitingByteCapacity,
    TotalRecordCapacityOverflow,
    TotalRetainedBytesOverflow,
    WaitingBytesOutOfRange,
    RecordCompletionMismatch,
    InsufficientBatchCapacity,
    InsufficientTimerCapacity,
    ZeroEncodedByteCapacity,
    ZeroWireBatchBytes,
    BatchRecordLimitExceedsCapacity,
    RetainedBytesOutOfRange,
    TransitionCapacityOverflow,
    UnexpectedCompressionWorkers,
    MissingCompressionWorkers,
    CompressionJobsExceedBatches,
}

impl fmt::Display for ProducerHostLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroRetainedBytes => "producer retained-byte capacity must be nonzero",
            Self::ZeroCompletionCapacity => "producer completion capacity must be nonzero",
            Self::ZeroWaitingRecordCapacity => "producer waiting record capacity must be nonzero",
            Self::ZeroWaitingByteCapacity => "producer waiting byte capacity must be nonzero",
            Self::TotalRecordCapacityOverflow => {
                "active and waiting producer record capacities overflow"
            }
            Self::TotalRetainedBytesOverflow => {
                "active and waiting producer byte capacities overflow"
            }
            Self::WaitingBytesOutOfRange => {
                "producer waiting-byte capacity exceeds the core byte domain"
            }
            Self::RecordCompletionMismatch => {
                "producer record and completion capacities must match"
            }
            Self::InsufficientBatchCapacity => {
                "producer batch capacity must cover every record slot"
            }
            Self::InsufficientTimerCapacity => {
                "producer timer capacity must cover every batch slot"
            }
            Self::ZeroEncodedByteCapacity => "producer encoded-byte capacity must be nonzero",
            Self::ZeroWireBatchBytes => "producer wire batch byte limit must be nonzero",
            Self::BatchRecordLimitExceedsCapacity => {
                "producer batch record limit exceeds record capacity"
            }
            Self::RetainedBytesOutOfRange => {
                "producer retained-byte capacity exceeds the core byte domain"
            }
            Self::TransitionCapacityOverflow => {
                "producer transition capacity exceeds the host domain"
            }
            Self::UnexpectedCompressionWorkers => {
                "uncompressed producer must not start compression workers"
            }
            Self::MissingCompressionWorkers => {
                "compressed producer requires bounded worker, job, and byte capacity"
            }
            Self::CompressionJobsExceedBatches => {
                "producer compression jobs exceed logical batch capacity"
            }
        })
    }
}

impl Error for ProducerHostLimitError {}

/// Failure to construct the producer host before any operation can be accepted.
#[derive(Debug)]
pub(crate) enum ProducerHostStartError {
    Limits(ProducerHostLimitError),
    Notifier(std::io::Error),
    Compression(std::io::Error),
}

impl From<ProducerHostLimitError> for ProducerHostStartError {
    fn from(error: ProducerHostLimitError) -> Self {
        Self::Limits(error)
    }
}

impl fmt::Display for ProducerHostStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limits(error) => error.fmt(formatter),
            Self::Notifier(error) => {
                write!(formatter, "producer notifier failed to start: {error}")
            }
            Self::Compression(error) => {
                write!(
                    formatter,
                    "producer compression workers failed to start: {error}"
                )
            }
        }
    }
}

impl Error for ProducerHostStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::Notifier(error) | Self::Compression(error) => Some(error),
        }
    }
}

/// Why ownership remained with the caller during normal admission rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerRejectionReason {
    Completion(CompletionRegistryError),
    Store(ProducerStoreError),
    Core(AdmissionRejection),
    Waiting(kafka_client_core::ProducerWaitingAdmissionError),
    HostPoisoned(ProducerHostInvariantError),
}

/// A supposedly impossible disagreement after deterministic acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerHostInvariantError {
    Core(ProducerMachineError),
    Store(ProducerStoreError),
    Binding(OperationBindingError),
    FlushBinding(FlushBindingError),
    Timer(BatchTimerError),
    Completion(CompletionRegistryError),
    Reclaim(CompletionReclaimError),
    Prepared(PreparedExecutionError),
    Compression(super::compression::CompressionPollError),
    Revision(ProducerRevisionError),
    MissingAdmissionIdentity,
    MissingCancellationOutcome,
    UnexpectedCancellationEffect,
    CommittedFactsMismatch,
    GeneratedFactCapacity,
    PendingEffectCapacity,
    TerminalBacklogCapacity,
    MissingFlushIdentity,
    UnexpectedDriverInput,
    WaitingOwnership,
    WaitingToken,
    #[cfg(test)]
    ForcedTerminalInterpretation,
    #[cfg(test)]
    ForcedTerminalPlanning,
}

impl Error for ProducerHostInvariantError {}
