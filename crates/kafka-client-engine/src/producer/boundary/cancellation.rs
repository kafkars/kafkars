//! Stable engine cancellation outcomes, errors, and advisory wake faults.

use std::{error::Error, fmt};

use kafka_client_core::ProducerCancellationOutcome as CoreCancellationOutcome;

use crate::producer::ingress::{
    ProducerPortCancelAccepted, ProducerPortCancelError, ProducerPortCancelFault,
};

/// Core-owned resolution of one immediate cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerCancellationOutcome {
    /// Core cancelled the operation before driver ownership.
    CancelledNotSent,
    /// Driver ownership already made per-record cancellation unsafe.
    TooLate,
    /// The operation is terminal or no longer retained by core.
    AlreadyTerminal,
}

/// Successfully interpreted cancellation decision and any wake fault.
#[derive(Debug)]
#[must_use = "a committed cancellation outcome must be observed"]
pub struct ProducerCancelAccepted {
    outcome: ProducerCancellationOutcome,
    fault: Option<ProducerCancelFault>,
}

impl ProducerCancelAccepted {
    /// Returns the deterministic stage-aware cancellation decision.
    pub const fn outcome(&self) -> ProducerCancellationOutcome {
        self.outcome
    }

    /// Returns a post-decision wake fault without revoking the outcome.
    pub const fn fault(&self) -> Option<&ProducerCancelFault> {
        self.fault.as_ref()
    }

    pub(crate) fn from_port(accepted: ProducerPortCancelAccepted) -> Self {
        let outcome = translate_outcome(accepted.outcome());
        let fault = accepted.into_fault().map(ProducerCancelFault::from_port);
        Self { outcome, fault }
    }
}

/// Category of a post-decision cancellation fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerCancelFaultKind {
    /// Cancellation committed, but requesting another host turn failed.
    Wake,
}

/// Advisory fault retained alongside a committed cancellation outcome.
#[derive(Debug)]
pub struct ProducerCancelFault {
    kind: ProducerCancelFaultKind,
    detail: String,
}

impl ProducerCancelFault {
    /// Returns the stable advisory category.
    pub const fn kind(&self) -> ProducerCancelFaultKind {
        self.kind
    }

    fn from_port(fault: ProducerPortCancelFault) -> Self {
        match fault {
            ProducerPortCancelFault::Wake(error) => Self {
                kind: ProducerCancelFaultKind::Wake,
                detail: error.to_string(),
            },
        }
    }
}

impl fmt::Display for ProducerCancelFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for ProducerCancelFault {}

/// Stable category for cancellation attempts lacking a core decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerCancelErrorKind {
    /// Another bounded shard operation currently owns the lock.
    Contended,
    /// The producer host no longer exists or cannot execute operations.
    HostUnavailable,
    /// The sealed execution generation space cannot advance safely.
    ExecutionGenerationExhausted,
    /// Engine and core cancellation ownership disagreed.
    InternalInvariant,
}

/// Immediate cancellation failure before an outcome becomes observable.
#[derive(Debug)]
pub struct ProducerCancelError {
    kind: ProducerCancelErrorKind,
    detail: String,
}

impl ProducerCancelError {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> ProducerCancelErrorKind {
        self.kind
    }

    pub(crate) fn from_port(error: ProducerPortCancelError) -> Self {
        match error {
            ProducerPortCancelError::Contended => Self::new(
                ProducerCancelErrorKind::Contended,
                "producer cancellation would block",
            ),
            ProducerPortCancelError::HostUnavailable => Self::new(
                ProducerCancelErrorKind::HostUnavailable,
                "producer cancellation host is unavailable",
            ),
            ProducerPortCancelError::ExecutionGenerationExhausted => Self::new(
                ProducerCancelErrorKind::ExecutionGenerationExhausted,
                "producer execution generation is exhausted",
            ),
            ProducerPortCancelError::InternalInvariant(error) => Self {
                kind: ProducerCancelErrorKind::InternalInvariant,
                detail: error.to_string(),
            },
        }
    }

    pub(crate) fn host_unavailable() -> Self {
        Self::new(
            ProducerCancelErrorKind::HostUnavailable,
            "producer cancellation capability is unavailable",
        )
    }

    fn new(kind: ProducerCancelErrorKind, detail: &str) -> Self {
        Self {
            kind,
            detail: detail.to_owned(),
        }
    }
}

impl fmt::Display for ProducerCancelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for ProducerCancelError {}

const fn translate_outcome(outcome: CoreCancellationOutcome) -> ProducerCancellationOutcome {
    match outcome {
        CoreCancellationOutcome::CancelledNotSent => ProducerCancellationOutcome::CancelledNotSent,
        CoreCancellationOutcome::TooLate => ProducerCancellationOutcome::TooLate,
        CoreCancellationOutcome::AlreadyTerminal => ProducerCancellationOutcome::AlreadyTerminal,
    }
}
