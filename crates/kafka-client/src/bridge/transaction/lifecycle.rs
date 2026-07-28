//! Linear bridge ownership for one active transaction and its accepted end.

use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use kafka_client_engine::{
    TransactionEndObserver as EngineEndObserver, TransactionToken as EngineTransactionToken,
};

use crate::{KafkaError, bridge::transaction::result::translate_control_kind};

use super::{TransactionalProducerEngine, result::translate_end_observation};

/// Private active transaction retaining the mutable engine-owner borrow.
pub(crate) struct TransactionEngine<'producer> {
    pub(super) inner: EngineTransactionToken<'producer>,
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
            begin_wake_failed,
        })
    }
}

impl<'producer> TransactionEngine<'producer> {
    pub(crate) const fn begin_wake_failed(&self) -> bool {
        self.begin_wake_failed
    }

    pub(crate) fn commit(
        self,
        timeout: Duration,
    ) -> Result<TransactionEndEngine<'producer>, (Self, KafkaError)> {
        self.end(timeout, TransactionEndIntent::Commit)
    }

    pub(crate) fn abort(
        self,
        timeout: Duration,
    ) -> Result<TransactionEndEngine<'producer>, (Self, KafkaError)> {
        self.end(timeout, TransactionEndIntent::Abort)
    }

    fn end(
        self,
        timeout: Duration,
        intent: TransactionEndIntent,
    ) -> Result<TransactionEndEngine<'producer>, (Self, KafkaError)> {
        let Self {
            inner,
            begin_wake_failed,
        } = self;
        let accepted = match intent {
            TransactionEndIntent::Commit => inner.commit(timeout),
            TransactionEndIntent::Abort => inner.abort(timeout),
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
                let semantic = translate_control_kind(error.kind());
                Err((
                    Self {
                        inner: error.into_transaction(),
                        begin_wake_failed,
                    },
                    semantic,
                ))
            }
        }
    }
}

impl core::fmt::Debug for TransactionEngine<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionEngine")
            .field("inner", &self.inner)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionEndIntent {
    Commit,
    Abort,
}
