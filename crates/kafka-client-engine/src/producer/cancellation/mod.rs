//! Stage-aware cancellation coordination without ambient time.

mod error;
mod host;
#[cfg(test)]
mod host_test;
mod revision;
#[cfg(test)]
mod revision_test;

pub(super) use error::ProducerRevisionError;
pub(super) use host::{ProducerHostCancelAccepted, ProducerHostCancelError};
