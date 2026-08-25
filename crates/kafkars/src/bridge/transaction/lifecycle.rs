//! Linear bridge ownership for one active transaction and its accepted end.

use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use kafka_client_engine::{
    TransactionEndObserver as EngineEndObserver, TransactionToken as EngineTransactionToken,
};

use crate::{ErrorKind, KafkaError, TransactionEndIntent};

use super::{
    TransactionalProducerEngine,
    identity::TransactionIdentityState,
    result::{translate_control_kind, translate_end_observation},
};

/// Private active transaction retaining the mutable engine-owner borrow.
pub(crate) struct TransactionEngine<'producer> {
    pub(super) inner: EngineTransactionToken<'producer>,
    pub(super) admin: crate::bridge::admin::AdminEngine,
    pub(super) identity: TransactionIdentityState,
    begin_wake_failed: bool,
}

impl TransactionalProducerEngine {
    pub(crate) fn begin(&mut self) -> Result<TransactionEngine<'_>, KafkaError> {
        let accepted = self
            .handle
            .begin_transaction()
            .map_err(|error| translate_control_kind(error.kind()))?;
        let begin_wake_failed = accepted.wake_failed();
        Ok(TransactionEngine {
            inner: accepted.into_transaction(),
            admin: self.admin.clone(),
            identity: TransactionIdentityState::new(),
            begin_wake_failed,
        })
    }
}

impl<'producer> TransactionEngine<'producer> {
    pub(crate) const fn begin_wake_failed(&self) -> bool {
        self.begin_wake_failed
    }

    #[expect(
        clippy::result_large_err,
        reason = "end rejection returns the exact active transaction owner for retry or abort"
    )]
    pub(crate) fn commit(
        self,
        deadline: Option<Instant>,
    ) -> Result<TransactionEndEngine<'producer>, (Self, KafkaError)> {
        let Some(deadline) = deadline else {
            return Err((self, invalid_end_deadline(TransactionEndIntent::Commit)));
        };
        if deadline <= Instant::now() {
            return Err((self, invalid_end_deadline(TransactionEndIntent::Commit)));
        }
        if self.identity.topic_mismatch() {
            return Err((
                self,
                KafkaError::new(
                    ErrorKind::Identity,
                    "transaction topic identity mismatch requires abort",
                )
                .with_transaction_end_intent(TransactionEndIntent::Commit)
                .with_transaction_abort_required(),
            ));
        }
        if self.identity.requires_validation() && !self.identity.is_sealed() {
            return Err((
                self,
                KafkaError::new(
                    ErrorKind::State,
                    "validate transaction topic identities before commit",
                )
                .with_transaction_end_intent(TransactionEndIntent::Commit),
            ));
        }
        if deadline <= Instant::now() {
            return Err((self, invalid_end_deadline(TransactionEndIntent::Commit)));
        }
        self.end(deadline, TransactionEndIntent::Commit)
    }

    #[expect(
        clippy::result_large_err,
        reason = "end rejection returns the exact active transaction owner for another abort"
    )]
    pub(crate) fn abort(
        self,
        deadline: Option<Instant>,
    ) -> Result<TransactionEndEngine<'producer>, (Self, KafkaError)> {
        let Some(deadline) = deadline else {
            return Err((self, invalid_end_deadline(TransactionEndIntent::Abort)));
        };
        if deadline <= Instant::now() {
            return Err((self, invalid_end_deadline(TransactionEndIntent::Abort)));
        }
        self.end(deadline, TransactionEndIntent::Abort)
    }

    #[expect(
        clippy::result_large_err,
        reason = "private end admission reconstructs and returns the exact active owner on rejection"
    )]
    fn end(
        self,
        deadline: Instant,
        intent: TransactionEndIntent,
    ) -> Result<TransactionEndEngine<'producer>, (Self, KafkaError)> {
        let Self {
            inner,
            admin,
            identity,
            begin_wake_failed,
        } = self;
        let accepted = match intent {
            TransactionEndIntent::Commit => inner.commit_until(deadline),
            TransactionEndIntent::Abort => inner.abort_until(deadline),
        };
        match accepted {
            Ok(accepted) => {
                let end_wake_failed = accepted.wake_failed();
                Ok(TransactionEndEngine {
                    inner: accepted.into_observer(),
                    intent,
                    begin_wake_failed,
                    end_wake_failed,
                    _producer: PhantomData,
                })
            }
            Err(error) => {
                let semantic =
                    translate_control_kind(error.kind()).with_transaction_end_intent(intent);
                Err((
                    Self {
                        inner: error.into_transaction(),
                        admin,
                        identity,
                        begin_wake_failed,
                    },
                    semantic,
                ))
            }
        }
    }
}

fn invalid_end_deadline(intent: TransactionEndIntent) -> KafkaError {
    let message = match intent {
        TransactionEndIntent::Commit => {
            "transaction commit deadline cannot be represented or has elapsed"
        }
        TransactionEndIntent::Abort => {
            "transaction abort deadline cannot be represented or has elapsed"
        }
    };
    KafkaError::new(ErrorKind::Configuration, message).with_transaction_end_intent(intent)
}

impl core::fmt::Debug for TransactionEngine<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionEngine")
            .field("inner", &self.inner)
            .field("identity", &self.identity)
            .field("begin_wake_failed", &self.begin_wake_failed)
            .finish()
    }
}

/// Private runtime-neutral observer retaining the producer borrow until drop.
pub(crate) struct TransactionEndEngine<'producer> {
    inner: EngineEndObserver,
    intent: TransactionEndIntent,
    begin_wake_failed: bool,
    end_wake_failed: bool,
    _producer: PhantomData<&'producer mut TransactionalProducerEngine>,
}

impl TransactionEndEngine<'_> {
    pub(crate) const fn begin_wake_failed(&self) -> bool {
        self.begin_wake_failed
    }

    pub(crate) const fn end_wake_failed(&self) -> bool {
        self.end_wake_failed
    }

    pub(crate) fn wait(self) -> Result<(), KafkaError> {
        translate_end_observation(self.intent, self.inner.wait())
    }
}

impl Future for TransactionEndEngine<'_> {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(|result| translate_end_observation(this.intent, result))
    }
}

impl core::fmt::Debug for TransactionEndEngine<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionEndEngine")
            .field("inner", &self.inner)
            .field("intent", &self.intent)
            .field("begin_wake_failed", &self.begin_wake_failed)
            .field("end_wake_failed", &self.end_wake_failed)
            .finish()
    }
}
