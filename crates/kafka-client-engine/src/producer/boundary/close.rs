//! Close-specific names over the shared producer barrier admission path.

use std::fmt;

use super::{
    flush_error::{ProducerTryFlushError, ProducerTryFlushErrorKind},
    flush_result::ProducerTryFlushAccepted,
};
use crate::producer::ingress::ProducerPortFlushError;

/// Accepted close ownership reuses the exact flush barrier observer.
pub type ProducerTryCloseAccepted = ProducerTryFlushAccepted;

/// Stable close rejection categories shared with bounded flush admission.
pub type ProducerTryCloseErrorKind = ProducerTryFlushErrorKind;

/// Immediate producer close admission failure.
#[derive(Debug)]
pub struct ProducerTryCloseError {
    inner: ProducerTryFlushError,
}

impl ProducerTryCloseError {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> ProducerTryCloseErrorKind {
        self.inner.kind()
    }

    /// Returns diagnostic detail for an internal mechanism fault.
    pub fn detail(&self) -> Option<&str> {
        self.inner.detail()
    }

    pub(super) const fn moment_unrepresentable() -> Self {
        Self {
            inner: ProducerTryFlushError::moment_unrepresentable(),
        }
    }

    pub(super) fn from_port(error: ProducerPortFlushError) -> Self {
        Self {
            inner: ProducerTryFlushError::from_port(error),
        }
    }
}

impl fmt::Display for ProducerTryCloseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.detail() {
            Some(detail) => write!(
                formatter,
                "producer try_close failed: {:?}: {detail}",
                self.kind()
            ),
            None => write!(formatter, "producer try_close failed: {:?}", self.kind()),
        }
    }
}

impl std::error::Error for ProducerTryCloseError {}
