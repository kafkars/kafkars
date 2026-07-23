//! Explicit configuration, rejection, and post-acceptance invariant failures.

use std::{error::Error, fmt};

use kafka_client_core::{AdmissionRejection, ProducerMachineError};

use crate::{clock::BatchTimerError, completion::CompletionRegistryError};

use super::{
    CompletionBindingError, ProducerStoreError, execution::PreparedExecutionError,
    reclaim::CompletionReclaimError,
};

/// Invalid synchronization between core and engine capacity owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerHostLimitError {
    ZeroRetainedBytes,
    ZeroCompletionCapacity,
    RecordCompletionMismatch,
    InsufficientBatchCapacity,
    InsufficientTimerCapacity,
    InsufficientNotificationCapacity,
    ZeroEncodedByteCapacity,
    ZeroWireBatchBytes,
    BatchRecordLimitExceedsCapacity,
    RetainedBytesOutOfRange,
}

impl fmt::Display for ProducerHostLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroRetainedBytes => "producer retained-byte capacity must be nonzero",
            Self::ZeroCompletionCapacity => "producer completion capacity must be nonzero",
            Self::RecordCompletionMismatch => {
                "producer record and completion capacities must match"
            }
            Self::InsufficientBatchCapacity => {
                "producer batch capacity must cover every record slot"
            }
            Self::InsufficientTimerCapacity => {
                "producer timer capacity must cover every batch slot"
            }
            Self::InsufficientNotificationCapacity => {
                "producer notification capacity must cover every completion slot"
            }
            Self::ZeroEncodedByteCapacity => "producer encoded-byte capacity must be nonzero",
            Self::ZeroWireBatchBytes => "producer wire batch byte limit must be nonzero",
            Self::BatchRecordLimitExceedsCapacity => {
                "producer batch record limit exceeds record capacity"
            }
            Self::RetainedBytesOutOfRange => {
                "producer retained-byte capacity exceeds the core byte domain"
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
        }
    }
}

impl Error for ProducerHostStartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::Notifier(error) => Some(error),
        }
    }
}

/// Why ownership remained with the caller during normal admission rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerRejectionReason {
    Completion(CompletionRegistryError),
    Store(ProducerStoreError),
    Core(AdmissionRejection),
    HostPoisoned(ProducerHostInvariantError),
}

/// A supposedly impossible disagreement after deterministic acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerHostInvariantError {
    Core(ProducerMachineError),
    Store(ProducerStoreError),
    Binding(CompletionBindingError),
    Timer(BatchTimerError),
    Completion(CompletionRegistryError),
    Reclaim(CompletionReclaimError),
    Prepared(PreparedExecutionError),
    MissingAdmissionIdentity,
    CommittedFactsMismatch,
    GeneratedFactCapacity,
    PendingEffectCapacity,
}

impl fmt::Display for ProducerHostInvariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(error) => write!(formatter, "core transition invariant failed: {error}"),
            Self::Store(error) => write!(formatter, "producer store invariant failed: {error}"),
            Self::Binding(error) => {
                write!(
                    formatter,
                    "producer completion binding invariant failed: {error}"
                )
            }
            Self::Timer(error) => write!(formatter, "producer timer invariant failed: {error}"),
            Self::Completion(error) => {
                write!(formatter, "producer completion invariant failed: {error}")
            }
            Self::Reclaim(error) => {
                write!(
                    formatter,
                    "producer completion reclaim invariant failed: {error}"
                )
            }
            Self::Prepared(error) => {
                write!(formatter, "prepared producer execution failed: {error}")
            }
            Self::MissingAdmissionIdentity => {
                formatter.write_str("accepted producer transition omitted its operation identity")
            }
            Self::CommittedFactsMismatch => {
                formatter.write_str("committed producer record facts changed after core admission")
            }
            Self::GeneratedFactCapacity => {
                formatter.write_str("producer generated-fact queue exceeded its fixed capacity")
            }
            Self::PendingEffectCapacity => {
                formatter.write_str("producer pending-effect storage exceeded its fixed capacity")
            }
        }
    }
}

impl Error for ProducerHostInvariantError {}
