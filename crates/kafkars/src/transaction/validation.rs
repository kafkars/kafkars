//! Named observation of fresh transaction topic-identity validation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::transaction::TransactionValidationEngine};

/// Sole validation observer borrowing one active transaction.
///
/// Dropping this value abandons observation and never installs a commit seal.
/// It does not cancel accepted `DescribeTopics` work. A later commit remains
/// rejected until a fresh complete validation succeeds; dropping the active
/// transaction instead retains its existing best-effort abort behavior.
#[derive(Debug)]
#[must_use = "wait for validation before attempting transaction commit"]
pub struct ValidateTransaction<'validation, 'producer> {
    inner: TransactionValidationEngine<'validation, 'producer>,
}

impl<'validation, 'producer> ValidateTransaction<'validation, 'producer> {
    pub(crate) const fn from_bridge(
        inner: TransactionValidationEngine<'validation, 'producer>,
    ) -> Self {
        Self { inner }
    }

    /// Blocks on the same one-shot terminal observed by [`Future::poll`].
    pub fn wait(self) -> Result<(), KafkaError> {
        self.inner.wait()
    }
}

impl Future for ValidateTransaction<'_, '_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(context)
    }
}
