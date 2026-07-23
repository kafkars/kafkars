//! Stable failures for immediate producer flush admission.

use std::fmt;

use kafka_client_core::FlushLedgerError;

use super::super::{flush::FlushRejectionReason, ingress::ProducerPortFlushError};
use crate::completion::CompletionRegistryError;

/// Stable reason a producer flush did not cross admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerTryFlushErrorKind {
    /// The engine monotonic call boundary cannot be represented.
    MomentUnrepresentable,
    /// Another thread currently owns this producer shard.
    Contended,
    /// Every shared terminal-completion slot is retained.
    CompletionCapacity,
    /// Producer admission or notification has closed.
    Closed,
    /// The bounded local flush identity domain is exhausted.
    LocalIdentityExhausted,
    /// The producer host stopped after an invariant failure.
    HostPoisoned,
    /// A non-semantic engine mechanism violated its internal contract.
    InternalInvariant,
}

/// Immediate producer flush admission failure.
#[derive(Debug)]
pub struct ProducerTryFlushError {
    kind: ProducerTryFlushErrorKind,
    detail: Option<String>,
}

impl ProducerTryFlushError {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> ProducerTryFlushErrorKind {
        self.kind
    }

    /// Returns diagnostic detail for an internal mechanism fault.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub(super) const fn moment_unrepresentable() -> Self {
        Self {
            kind: ProducerTryFlushErrorKind::MomentUnrepresentable,
            detail: None,
        }
    }

    pub(super) fn from_port(error: ProducerPortFlushError) -> Self {
        match error {
            ProducerPortFlushError::Contended => Self::simple(ProducerTryFlushErrorKind::Contended),
            ProducerPortFlushError::ShardPoisoned => {
                Self::simple(ProducerTryFlushErrorKind::HostPoisoned)
            }
            ProducerPortFlushError::Rejected(reason) => Self::simple(map_rejection(reason)),
            ProducerPortFlushError::HostInvariant(error) => Self {
                kind: ProducerTryFlushErrorKind::InternalInvariant,
                detail: Some(error.to_string()),
            },
        }
    }

    const fn simple(kind: ProducerTryFlushErrorKind) -> Self {
        Self { kind, detail: None }
    }
}

impl fmt::Display for ProducerTryFlushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            Some(detail) => write!(
                formatter,
                "producer try_flush failed: {:?}: {detail}",
                self.kind
            ),
            None => write!(formatter, "producer try_flush failed: {:?}", self.kind),
        }
    }
}

impl std::error::Error for ProducerTryFlushError {}

const fn map_rejection(reason: FlushRejectionReason) -> ProducerTryFlushErrorKind {
    match reason {
        FlushRejectionReason::Completion(CompletionRegistryError::Full)
        | FlushRejectionReason::Core(FlushLedgerError::Capacity) => {
            ProducerTryFlushErrorKind::CompletionCapacity
        }
        FlushRejectionReason::Completion(CompletionRegistryError::NotifierStopped)
        | FlushRejectionReason::Closed => ProducerTryFlushErrorKind::Closed,
        FlushRejectionReason::Core(FlushLedgerError::IdentityExhausted) => {
            ProducerTryFlushErrorKind::LocalIdentityExhausted
        }
        FlushRejectionReason::HostPoisoned(_) => ProducerTryFlushErrorKind::HostPoisoned,
        FlushRejectionReason::Completion(_)
        | FlushRejectionReason::Core(
            FlushLedgerError::UnknownFlush | FlushLedgerError::NotCompleted,
        ) => ProducerTryFlushErrorKind::InternalInvariant,
    }
}
