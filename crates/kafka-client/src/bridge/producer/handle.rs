//! Lossless record ownership transfer across the private facade-engine seam.

use std::time::Duration;

use kafka_client_engine::{
    ProducerHandle as EngineProducerHandle, ProducerSendOptions as EngineProducerSendOptions,
};

use crate::{
    bridge::{
        producer_result::admission::{
            ProducerAdmissionRejection, translate_accepted_fault, translate_admission_error,
            translate_batch_admission_error, translate_batch_capture_error,
            translate_capture_error,
        },
        producer_result::{close::translate_close_admission, flush::translate_flush_admission},
    },
    record::Record,
};

use super::{
    barrier::{BarrierKind, ProducerBarrier},
    batch::ProducerBatch,
    delivery::ProducerDelivery,
    into_engine_record, restore_rejected_record,
    send::ProducerSend,
};

/// Cloneable facade-to-engine producer owner with one compiled timeout.
#[derive(Debug, Clone)]
pub(crate) struct ProducerEngine {
    handle: EngineProducerHandle,
    options: EngineProducerSendOptions,
}

impl ProducerEngine {
    pub(crate) fn new(handle: EngineProducerHandle, delivery_timeout: Duration) -> Self {
        Self {
            handle,
            options: EngineProducerSendOptions::new(delivery_timeout),
        }
    }

    pub(crate) const fn with_delivery_timeout(mut self, delivery_timeout: Duration) -> Self {
        self.options = EngineProducerSendOptions::new(delivery_timeout);
        self
    }

    pub(crate) const fn delivery_timeout(&self) -> Duration {
        self.options.delivery_timeout()
    }

    /// Captures one exact barrier or returns its admission error as ready state.
    pub(crate) fn flush(&self) -> ProducerBarrier {
        match self.handle.try_flush() {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                ProducerBarrier::accepted(BarrierKind::Flush, accepted.into_observer(), diagnostic)
            }
            Err(error) => {
                ProducerBarrier::ready(BarrierKind::Flush, Err(translate_flush_admission(&error)))
            }
        }
    }

    /// Attempts one atomic close and returns its terminal authority.
    pub(crate) fn close(&self) -> ProducerBarrier {
        match self.handle.try_close() {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                ProducerBarrier::accepted(BarrierKind::Close, accepted.into_observer(), diagnostic)
            }
            Err(error) => {
                ProducerBarrier::ready(BarrierKind::Close, Err(translate_close_admission(&error)))
            }
        }
    }

    /// Captures the public boundary before converting caller-owned bytes.
    #[allow(
        clippy::result_large_err,
        reason = "pre-admission failure returns the exact facade record"
    )]
    pub(crate) fn try_send(
        &self,
        record: Record,
    ) -> Result<ProducerDelivery, ProducerAdmissionRejection> {
        let capture = match self.handle.capture_send(self.options) {
            Ok(capture) => capture,
            Err(error) => return Err(translate_capture_error(record, error)),
        };
        let topic = record.topic().to_owned();
        let create_timestamp = record
            .timestamp()
            .unwrap_or_else(|| capture.default_timestamp_milliseconds());
        let serialized_key_size = record.key_bytes().map(bytes::Bytes::len);
        let serialized_value_size = record.value_bytes().map(bytes::Bytes::len);
        let engine_record = into_engine_record(record);
        match self.handle.try_send_captured(capture, engine_record) {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                Ok(ProducerDelivery::new(
                    topic,
                    create_timestamp,
                    serialized_key_size,
                    serialized_value_size,
                    accepted.into_observer(),
                    diagnostic,
                ))
            }
            Err(error) => Err(translate_admission_error(error)),
        }
    }

    /// Captures the call boundary and enters bounded FIFO waiting ownership.
    pub(crate) fn send(&self, record: Record) -> ProducerSend {
        let capture = match self.handle.capture_send(self.options) {
            Ok(capture) => capture,
            Err(error) => {
                let (_record, error) = translate_capture_error(record, error).into_parts();
                return ProducerSend::ready(error);
            }
        };
        let topic = record.topic().to_owned();
        let create_timestamp = record
            .timestamp()
            .unwrap_or_else(|| capture.default_timestamp_milliseconds());
        let serialized_key_size = record.key_bytes().map(bytes::Bytes::len);
        let serialized_value_size = record.value_bytes().map(bytes::Bytes::len);
        let engine_record = into_engine_record(record);
        match self.handle.send_captured(capture, engine_record) {
            Ok(accepted) => {
                let diagnostic = accepted.fault().map(translate_accepted_fault);
                ProducerSend::accepted(ProducerDelivery::new(
                    topic,
                    create_timestamp,
                    serialized_key_size,
                    serialized_value_size,
                    accepted.into_observer(),
                    diagnostic,
                ))
            }
            Err(error) => {
                let (_record, error) = translate_admission_error(error).into_parts();
                ProducerSend::ready(error)
            }
        }
    }

    /// Captures one batch boundary before record conversion.
    pub(crate) fn send_batch(&self, records: Vec<Record>) -> ProducerBatch {
        let capture = match self.handle.capture_batch(self.options) {
            Ok(capture) => capture,
            Err(error) => {
                return ProducerBatch::new(
                    Vec::new(),
                    Some(crate::TrySendError::new(
                        records,
                        translate_batch_capture_error(error),
                    )),
                );
            }
        };
        if records.len() > self.handle.batch_admission_capacity() {
            return ProducerBatch::new(
                Vec::new(),
                Some(crate::TrySendError::new(
                    records,
                    translate_batch_admission_error(
                        kafka_client_engine::ProducerTrySendErrorKind::RecordCapacity,
                        None,
                    ),
                )),
            );
        }
        let default_timestamp = capture.default_timestamp_milliseconds();
        let metadata_contexts = records
            .iter()
            .map(|record| {
                (
                    record.topic().to_owned(),
                    record.timestamp().unwrap_or(default_timestamp),
                    record.key_bytes().map(bytes::Bytes::len),
                    record.value_bytes().map(bytes::Bytes::len),
                )
            })
            .collect::<Vec<_>>();
        let engine_records = records.into_iter().map(into_engine_record).collect();
        let (accepted, rejection) = self
            .handle
            .try_send_batch_captured(capture, engine_records)
            .into_parts();
        let deliveries = metadata_contexts
            .into_iter()
            .zip(accepted)
            .map(
                |(
                    (topic, create_timestamp, serialized_key_size, serialized_value_size),
                    accepted,
                )| {
                    let diagnostic = accepted.fault().map(translate_accepted_fault);
                    ProducerDelivery::new(
                        topic,
                        create_timestamp,
                        serialized_key_size,
                        serialized_value_size,
                        accepted.into_observer(),
                        diagnostic,
                    )
                },
            )
            .collect();
        let rejection = rejection.map(|error| {
            let (kind, records, detail) = error.into_parts();
            let records = records.into_iter().map(restore_rejected_record).collect();
            crate::TrySendError::new(
                records,
                translate_batch_admission_error(kind, detail.as_deref()),
            )
        });
        ProducerBatch::new(deliveries, rejection)
    }
}
