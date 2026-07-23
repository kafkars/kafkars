//! Engine-owned terminal delivery and observer-lifecycle failure vocabulary.

use core::fmt;

use crate::completion::CompletionObserverError;

use super::ProducerDeliveryFailure;

/// Failure in the single-observer completion lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerObserverError {
    /// The terminal value was already taken by this observer.
    AlreadyObserved,
    /// The observer's completion generation is no longer live.
    Stale,
}

impl ProducerObserverError {
    pub(crate) const fn from_completion(error: CompletionObserverError) -> Self {
        match error {
            CompletionObserverError::AlreadyObserved => Self::AlreadyObserved,
            CompletionObserverError::Stale => Self::Stale,
        }
    }
}

impl fmt::Display for ProducerObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyObserved => "producer delivery was already observed",
            Self::Stale => "producer delivery observer is stale",
        })
    }
}

impl std::error::Error for ProducerObserverError {}

/// Terminal producer failure or failure to observe its completion cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerDeliveryError {
    /// Kafka delivery ended with semantic failure and delivery certainty.
    Failed(ProducerDeliveryFailure),
    /// The single-observer lifecycle rejected observation.
    Observer(ProducerObserverError),
}

impl fmt::Display for ProducerDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(failure) => {
                write!(formatter, "producer delivery failed: {:?}", failure.kind())
            }
            Self::Observer(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProducerDeliveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed(_) => None,
            Self::Observer(error) => Some(error),
        }
    }
}
