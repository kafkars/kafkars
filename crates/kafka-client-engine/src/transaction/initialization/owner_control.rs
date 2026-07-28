//! Linear public begin, commit, and abort ownership over one initialized owner.

use std::{fmt, time::Duration};

use kafka_client_core::TransactionEpoch;

use super::{
    TransactionControlError, TransactionEndAdmissionError, TransactionEndObserver,
    TransactionLifecycleControlAccepted, TransactionalOwnerHandle,
    control_error_mapping::{control_error, control_error_kind},
};

impl TransactionalOwnerHandle {
    /// Begins one transaction and returns its opaque linear token.
    pub fn begin_transaction(
        &mut self,
    ) -> Result<TransactionBeginAccepted<'_>, TransactionControlError> {
        let accepted = self.begin().map_err(control_error)?;
        Ok(TransactionBeginAccepted {
            transaction: TransactionToken::new(self, accepted.value),
            wake_failed: accepted.wake_failed,
        })
    }
}

/// Accepted begin ownership plus advisory post-admission wake status.
#[must_use = "accepted begin retains the sole active transaction token"]
pub struct TransactionBeginAccepted<'owner> {
    transaction: TransactionToken<'owner>,
    wake_failed: bool,
}

impl<'owner> TransactionBeginAccepted<'owner> {
    /// Reports that the advisory reactor wake failed after begin acceptance.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole opaque active transaction token.
    pub fn into_transaction(self) -> TransactionToken<'owner> {
        self.transaction
    }
}

impl fmt::Debug for TransactionBeginAccepted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionBeginAccepted")
            .field("wake_failed", &self.wake_failed)
            .finish_non_exhaustive()
    }
}

/// Opaque non-clone token for exactly one active transaction epoch.
#[must_use = "commit, abort, or drop the active transaction token"]
pub struct TransactionToken<'owner> {
    owner: &'owner mut TransactionalOwnerHandle,
    epoch: TransactionEpoch,
    armed: bool,
}

impl<'owner> TransactionToken<'owner> {
    const fn new(owner: &'owner mut TransactionalOwnerHandle, epoch: TransactionEpoch) -> Self {
        Self {
            owner,
            epoch,
            armed: true,
        }
    }

    /// Attempts to commit this exact transaction.
    pub fn commit(
        self,
        timeout: Duration,
    ) -> Result<TransactionEndAccepted, TransactionEndAdmissionError<'owner>> {
        self.end(timeout, TransactionEndIntent::Commit)
    }

    /// Attempts to abort this exact transaction.
    pub fn abort(
        self,
        timeout: Duration,
    ) -> Result<TransactionEndAccepted, TransactionEndAdmissionError<'owner>> {
        self.end(timeout, TransactionEndIntent::Abort)
    }

    #[cfg(test)]
    pub(in crate::transaction) const fn epoch(&self) -> TransactionEpoch {
        self.epoch
    }

    fn end(
        mut self,
        timeout: Duration,
        intent: TransactionEndIntent,
    ) -> Result<TransactionEndAccepted, TransactionEndAdmissionError<'owner>> {
        let lifetime = self.owner.lifetime();
        let result = match intent {
            TransactionEndIntent::Commit => self.owner.commit(self.epoch, timeout),
            TransactionEndIntent::Abort => self.owner.abort(self.epoch, timeout),
        };
        match result {
            Ok(TransactionLifecycleControlAccepted { value, wake_failed }) => {
                self.armed = false;
                Ok(TransactionEndAccepted {
                    observer: TransactionEndObserver::new(value, lifetime),
                    wake_failed,
                })
            }
            Err(error) => Err(TransactionEndAdmissionError::new(
                control_error_kind(error),
                self,
            )),
        }
    }
}

impl Drop for TransactionToken<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.armed = false;
            self.owner.lose_owner();
        }
    }
}

impl fmt::Debug for TransactionToken<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionToken")
            .field("transactional_id", &self.owner.transactional_id())
            .finish_non_exhaustive()
    }
}

/// Accepted explicit end ownership plus advisory post-admission wake status.
#[must_use = "accepted transaction end retains its sole terminal observer"]
pub struct TransactionEndAccepted {
    observer: TransactionEndObserver,
    wake_failed: bool,
}

impl TransactionEndAccepted {
    /// Reports that the advisory reactor wake failed after end acceptance.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole runtime-neutral end observer.
    pub fn into_observer(self) -> TransactionEndObserver {
        self.observer
    }
}

impl fmt::Debug for TransactionEndAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionEndAccepted")
            .field("observer", &self.observer)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}

#[derive(Clone, Copy)]
enum TransactionEndIntent {
    Commit,
    Abort,
}
