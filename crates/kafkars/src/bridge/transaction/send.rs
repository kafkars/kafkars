//! Linear bridge ownership for one accepted transactional record send.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use kafka_client_engine::TransactionSendObserver as EngineTransactionSendObserver;

use crate::{
    DeliveryStatus, KafkaError, Record, RecordMetadata, bridge::producer::prepare_engine_record,
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
        let prepared_identity = match self
            .identity
            .prepare_mutation(Some((record.topic(), record.expected_topic_uuid_value())))
        {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err((record, error.with_delivery_status(DeliveryStatus::NotSent)));
            }
        };
        let serialized_key_size = record.key_bytes().map(bytes::Bytes::len);
        let serialized_value_size = record.value_bytes().map(bytes::Bytes::len);
        let prepared = match prepare_engine_record(record) {
            Ok(prepared) => prepared,
            Err(record) => {
                return Err((
                    record,
                    translate_send_admission(
                        kafka_client_engine::TransactionSendAdmissionErrorKind::Allocation,
                    ),
                ));
            }
        };
        let (record, engine_record) = prepared.into_parts();
        match self.inner.send_captured(engine_record, capture) {
            Ok(accepted) => {
                drop(record);
                self.identity.commit_mutation(prepared_identity);
                let wake_failed = accepted.wake_failed();
                Ok(TransactionSendEngine {
                    inner: accepted.into_observer(),
                    wake_failed,
                    serialized_key_size,
                    serialized_value_size,
                })
            }
            Err(error) => {
                let semantic = translate_send_admission(error.kind());
                drop(error.into_record());
                Err((record, semantic))
            }
        }
    }
}

/// Private observer retaining both the transaction and producer-owner borrows.
pub(crate) struct TransactionSendEngine<'send, 'producer> {
    inner: EngineTransactionSendObserver<'send, 'producer>,
    wake_failed: bool,
    serialized_key_size: Option<usize>,
    serialized_value_size: Option<usize>,
}

impl TransactionSendEngine<'_, '_> {
    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    pub(crate) fn wait(self) -> Result<RecordMetadata, KafkaError> {
        translate_send_observation(
            self.inner.wait(),
            self.serialized_key_size,
            self.serialized_value_size,
        )
    }
}

impl Future for TransactionSendEngine<'_, '_> {
    type Output = Result<RecordMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context).map(|result| {
            translate_send_observation(result, this.serialized_key_size, this.serialized_value_size)
        })
    }
}

impl core::fmt::Debug for TransactionSendEngine<'_, '_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionSendEngine")
            .field("inner", &self.inner)
            .field("wake_failed", &self.wake_failed)
            .field("serialized_key_size", &self.serialized_key_size)
            .field("serialized_value_size", &self.serialized_value_size)
            .finish()
    }
}
