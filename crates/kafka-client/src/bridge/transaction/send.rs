//! Linear bridge ownership for one accepted transactional record send.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use kafka_client_engine::TransactionSendObserver as EngineTransactionSendObserver;

use crate::{
    KafkaError, Record, RecordMetadata,
    bridge::producer::{into_engine_record, restore_rejected_record},
};

use super::{
    TransactionEngine,
    send_result::{translate_send_admission, translate_send_capture, translate_send_observation},
};

impl<'producer> TransactionEngine<'producer> {
    #[expect(
        clippy::result_large_err,
        reason = "pre-admission rejection must return the exact caller record"
    )]
    pub(crate) fn send<'send>(
        &'send mut self,
        record: Record,
        timeout: Duration,
    ) -> Result<TransactionSendEngine<'send, 'producer>, (Record, KafkaError)> {
        let capture = match self.inner.capture_send(timeout) {
            Ok(capture) => capture,
            Err(error) => return Err((record, translate_send_capture(error))),
        };
        match self
            .inner
            .send_captured(into_engine_record(record), capture)
        {
            Ok(accepted) => {
                let wake_failed = accepted.wake_failed();
                Ok(TransactionSendEngine {
                    inner: accepted.into_observer(),
                    wake_failed,
                })
            }
            Err(error) => {
                let semantic = translate_send_admission(error.kind());
                let record = restore_rejected_record(error.into_record());
                Err((record, semantic))
            }
        }
    }
}

/// Private observer retaining both the transaction and producer-owner borrows.
pub(crate) struct TransactionSendEngine<'send, 'producer> {
    inner: EngineTransactionSendObserver<'send, 'producer>,
    wake_failed: bool,
}

impl TransactionSendEngine<'_, '_> {
    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    pub(crate) fn wait(self) -> Result<RecordMetadata, KafkaError> {
        translate_send_observation(self.inner.wait())
    }
}

impl Future for TransactionSendEngine<'_, '_> {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(translate_send_observation)
    }
}

impl core::fmt::Debug for TransactionSendEngine<'_, '_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionSendEngine")
            .field("inner", &self.inner)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}
