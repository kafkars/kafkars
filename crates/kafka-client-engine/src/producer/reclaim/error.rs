//! Failures in the two-phase producer completion reclaim handshake.

use std::{error::Error, fmt};

use crate::completion::{CompletionId, CompletionRegistryError};

use super::super::{binding::OperationBindingError, flush::FlushBindingError};

/// Failure while preserving the two-phase completion ownership handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionReclaimError {
    InvalidPhase,
    UnknownBinding(CompletionId),
    AmbiguousBinding(CompletionId),
    BindingMismatch,
    Registry(CompletionRegistryError),
    Binding(OperationBindingError),
    FlushBinding(FlushBindingError),
}

impl fmt::Display for CompletionReclaimError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPhase => formatter.write_str("completion reclaim phase is invalid"),
            Self::UnknownBinding(_) => {
                formatter.write_str("reclaim-ready completion has no producer binding")
            }
            Self::AmbiguousBinding(_) => {
                formatter.write_str("reclaim-ready completion has multiple producer bindings")
            }
            Self::BindingMismatch => {
                formatter.write_str("completion reclaim binding generation changed")
            }
            Self::Registry(error) => {
                write!(formatter, "completion registry rejected reclaim: {error}")
            }
            Self::Binding(error) => {
                write!(formatter, "completion binding rejected reclaim: {error}")
            }
            Self::FlushBinding(error) => {
                write!(
                    formatter,
                    "flush completion binding rejected reclaim: {error}"
                )
            }
        }
    }
}

impl Error for CompletionReclaimError {}

impl From<CompletionRegistryError> for CompletionReclaimError {
    fn from(error: CompletionRegistryError) -> Self {
        Self::Registry(error)
    }
}
