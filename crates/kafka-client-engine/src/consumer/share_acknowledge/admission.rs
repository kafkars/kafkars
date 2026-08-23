//! Capture-first public admission of one exact linear acknowledgement capability.

use std::{fmt, sync::Arc, time::Duration};

use crate::consumer::{
    ShareAcknowledgement,
    share::{
        ShareAcknowledgementPortAdmission, ShareAcknowledgementPortFailureSource,
        ShareConsumerHandle,
    },
};

use super::ShareAcknowledgementObserver;

/// Stable reason an acknowledgement did not enter terminal engine ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcknowledgementAdmissionErrorKind {
    /// The timeout could not form a positive absolute deadline.
    InvalidDeadline,
    /// The acknowledgement came from another engine registry.
    ForeignRegistry,
    /// Share admission or terminal notification has closed.
    Closed,
    /// Another owner currently holds the bounded share registry.
    Contended,
    /// The registered member or broker session is no longer available.
    Unavailable,
    /// Fixed operation or completion capacity is temporarily full.
    Backpressure,
    /// The captured public deadline elapsed before admission.
    DeadlineElapsed,
    /// The capability no longer matches the live session or acquisition ledger.
    StaleAcknowledgement,
    /// The normalized capability could not form a bounded protocol request.
    InvalidRequest,
    /// Engine completion or rollback ownership became inconsistent.
    Internal,
}

/// Pre-admission rejection retaining the exact caller acknowledgement.
#[must_use = "acknowledgement rejection retains the exact linear capability"]
pub struct ShareAcknowledgementAdmissionError {
    kind: ShareAcknowledgementAdmissionErrorKind,
    acknowledgement: ShareAcknowledgement,
}

impl ShareAcknowledgementAdmissionError {
    /// Returns the stable pre-admission rejection category.
    pub const fn kind(&self) -> ShareAcknowledgementAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact acknowledgement that never entered transport ownership.
    pub fn into_acknowledgement(self) -> ShareAcknowledgement {
        self.acknowledgement
    }
}

impl fmt::Debug for ShareAcknowledgementAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementAdmissionError")
            .field("kind", &self.kind)
            .field("acknowledgement", &self.acknowledgement)
            .finish()
    }
}

impl fmt::Display for ShareAcknowledgementAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "share acknowledgement rejected: {:?}", self.kind)
    }
}

impl std::error::Error for ShareAcknowledgementAdmissionError {}

/// Accepted acknowledgement ownership plus advisory wake diagnostics.
#[must_use = "accepted acknowledgement retains its sole terminal observer"]
pub struct ShareAcknowledgementAccepted {
    observer: ShareAcknowledgementObserver,
    wake_failed: bool,
}

impl ShareAcknowledgementAccepted {
    /// Reports that the post-admission reactor wake failed.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole runtime-neutral terminal observer.
    pub fn into_observer(self) -> ShareAcknowledgementObserver {
        self.observer
    }
}

impl fmt::Debug for ShareAcknowledgementAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementAccepted")
            .field("observer", &self.observer)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}

impl ShareConsumerHandle {
    /// Attempts one bounded session-fenced `ShareAcknowledge` operation.
    pub fn try_acknowledge(
        &self,
        acknowledgement: ShareAcknowledgement,
        timeout: Duration,
    ) -> Result<ShareAcknowledgementAccepted, ShareAcknowledgementAdmissionError> {
        let admission = self
            .port
            .try_acknowledge(self.group_id, acknowledgement, timeout)
            .map_err(|failure| ShareAcknowledgementAdmissionError {
                kind: admission_error_kind(failure.source),
                acknowledgement: failure.acknowledgement,
            })?;
        Ok(public_admission(admission, Arc::clone(&self.lifetime)))
    }
}

fn public_admission(
    admission: ShareAcknowledgementPortAdmission,
    lifetime: Arc<dyn Send + Sync>,
) -> ShareAcknowledgementAccepted {
    ShareAcknowledgementAccepted {
        observer: ShareAcknowledgementObserver::new(admission.observer, lifetime),
        wake_failed: admission.wake.is_some(),
    }
}

const fn admission_error_kind(
    source: ShareAcknowledgementPortFailureSource,
) -> ShareAcknowledgementAdmissionErrorKind {
    match source {
        ShareAcknowledgementPortFailureSource::InvalidDeadline => {
            ShareAcknowledgementAdmissionErrorKind::InvalidDeadline
        }
        ShareAcknowledgementPortFailureSource::ForeignRegistry => {
            ShareAcknowledgementAdmissionErrorKind::ForeignRegistry
        }
        ShareAcknowledgementPortFailureSource::Closed => {
            ShareAcknowledgementAdmissionErrorKind::Closed
        }
        ShareAcknowledgementPortFailureSource::Contended => {
            ShareAcknowledgementAdmissionErrorKind::Contended
        }
        ShareAcknowledgementPortFailureSource::Unavailable => {
            ShareAcknowledgementAdmissionErrorKind::Unavailable
        }
        ShareAcknowledgementPortFailureSource::Backpressure => {
            ShareAcknowledgementAdmissionErrorKind::Backpressure
        }
        ShareAcknowledgementPortFailureSource::DeadlineElapsed => {
            ShareAcknowledgementAdmissionErrorKind::DeadlineElapsed
        }
        ShareAcknowledgementPortFailureSource::Stale => {
            ShareAcknowledgementAdmissionErrorKind::StaleAcknowledgement
        }
        ShareAcknowledgementPortFailureSource::InvalidRequest => {
            ShareAcknowledgementAdmissionErrorKind::InvalidRequest
        }
        ShareAcknowledgementPortFailureSource::Internal => {
            ShareAcknowledgementAdmissionErrorKind::Internal
        }
    }
}
