//! Linear bridge ownership for one accepted homogeneous transactional batch.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use kafka_client_engine::{
    TransactionBatchSendObserver as EngineTransactionBatchSendObserver,
    TransactionSendAdmissionErrorKind,
};

use crate::{
    DeliveryStatus, KafkaError, Record,
    bridge::producer::{PreparedEngineRecords, prepare_engine_records as prepare_engine_mirrors},
    transaction::TransactionBatchMetadata,
};

use super::{
    TransactionEngine,
    send_result::{
        translate_send_admission, translate_send_batch_observation, translate_send_capture,
    },
};

impl<'producer> TransactionEngine<'producer> {
    pub(crate) fn send_batch<'send>(
        &'send mut self,
        records: Vec<Record>,
        timeout: Duration,
    ) -> Result<TransactionBatchSendEngine<'send, 'producer>, (Vec<Record>, KafkaError)> {
        let capture = match self.inner.capture_send(timeout) {
            Ok(capture) => capture,
            Err(error) => return Err((records, translate_send_capture(error))),
        };
        let capacity = self.inner.batch_record_capacity();
        if records.is_empty() || records.len() > capacity {
            return match prepare_engine_records(records, capacity) {
                Err(error) => Err(error),
                Ok(_records) => unreachable!("invalid batch count cannot convert"),
            };
        }
        let prepared_identity = match self.identity.prepare_mutation(
            records
                .first()
                .map(|record| (record.topic(), record.expected_topic_uuid_value())),
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err((records, error.with_delivery_status(DeliveryStatus::NotSent)));
            }
        };
        let prepared = prepare_engine_records(records, capacity)?;
        let (records, engine_records) = prepared.into_parts();
        match self.inner.send_batch_captured(engine_records, capture) {
            Ok(accepted) => {
                drop(records);
                self.identity.commit_mutation(prepared_identity);
                let wake_failed = accepted.wake_failed();
                Ok(TransactionBatchSendEngine {
                    inner: accepted.into_observer(),
                    wake_failed,
                })
            }
            Err(error) => {
                let semantic = translate_send_admission(error.kind());
                drop(error.into_records());
                Err((records, semantic))
            }
        }
    }
}

pub(super) fn prepare_engine_records(
    records: Vec<Record>,
    capacity: usize,
) -> Result<PreparedEngineRecords, (Vec<Record>, KafkaError)> {
    let record_count = records.len();
    let kind = if record_count == 0 {
        Some(TransactionSendAdmissionErrorKind::EmptyBatch)
    } else if record_count > capacity {
        Some(TransactionSendAdmissionErrorKind::BatchRecordCapacity {
            actual: record_count,
            limit: capacity,
        })
    } else {
        None
    };
    if let Some(kind) = kind {
        return Err((records, translate_send_admission(kind)));
    }
    prepare_engine_mirrors(records).map_err(|records| {
        (
            records,
            translate_send_admission(TransactionSendAdmissionErrorKind::Allocation),
        )
    })
}

/// Private observer retaining both the transaction and producer-owner borrows.
pub(crate) struct TransactionBatchSendEngine<'send, 'producer> {
    inner: EngineTransactionBatchSendObserver<'send, 'producer>,
    wake_failed: bool,
}

impl TransactionBatchSendEngine<'_, '_> {
    pub(crate) const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    pub(crate) fn wait(self) -> Result<TransactionBatchMetadata, KafkaError> {
        translate_send_batch_observation(self.inner.wait())
    }
}

impl Future for TransactionBatchSendEngine<'_, '_> {
    type Output = Result<TransactionBatchMetadata, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll(context)
            .map(translate_send_batch_observation)
    }
}

impl core::fmt::Debug for TransactionBatchSendEngine<'_, '_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("TransactionBatchSendEngine")
            .field("inner", &self.inner)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}
