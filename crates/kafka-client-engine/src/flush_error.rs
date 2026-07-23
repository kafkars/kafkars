//! Engine-owned producer flush terminal failure vocabulary.

use core::fmt;

use crate::ProducerObserverError;

/// Terminal producer flush failure or observer-lifecycle error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerFlushError {
    /// The engine permanently lost the ability to execute accepted work.
    ExecutionUnavailable,
    /// The single-observer lifecycle rejected observation.
    Observer(ProducerObserverError),
}

impl fmt::Display for ProducerFlushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionUnavailable => {
                formatter.write_str("producer execution became unavailable before flush")
            }
            Self::Observer(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProducerFlushError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ExecutionUnavailable => None,
            Self::Observer(error) => Some(error),
        }
    }
}
