//! Atomic producer-batch admission and per-record delivery reconstruction.

use super::ProducerEngine;
use crate::{
    Record,
    bridge::{
        producer::{
            batch::ProducerBatch, conversion::validate_batch_records, delivery::ProducerDelivery,
            into_engine_record, restore_rejected_record,
        },
        producer_result::admission::{
            translate_accepted_fault, translate_batch_admission_error,
            translate_batch_capture_error,
        },
    },
};

impl ProducerEngine {
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
        if records.is_empty() {
            return ProducerBatch::new(Vec::new(), None);
        }
        if let Err(kind) = validate_batch_records(&records) {
            return ProducerBatch::new(
                Vec::new(),
                Some(crate::TrySendError::new(
                    records,
                    translate_batch_admission_error(kind, None),
                )),
            );
        }
        let admission = match self.handle.try_begin_batch_admission() {
            Ok(admission) => admission,
            Err(kind) => {
                return ProducerBatch::new(
                    Vec::new(),
                    Some(crate::TrySendError::new(
                        records,
                        translate_batch_admission_error(kind, None),
                    )),
                );
            }
        };
        let default_timestamp = capture.default_timestamp_milliseconds();
        let metadata_contexts = records
            .iter()
            .map(|record| {
                (
                    std::sync::Arc::clone(record.topic_owner()),
                    record.expected_topic_uuid_value(),
                    record.timestamp().unwrap_or(default_timestamp),
                    record.key_bytes().map(bytes::Bytes::len),
                    record.value_bytes().map(bytes::Bytes::len),
                )
            })
            .collect::<Vec<_>>();
        let engine_records = records.into_iter().map(into_engine_record).collect();
        let outcome = admission.try_send_captured(capture, engine_records);
        let (accepted, rejection) = outcome.into_parts();
        let deliveries = metadata_contexts
            .into_iter()
            .zip(accepted)
            .map(
                |(
                    (
                        topic,
                        topic_uuid,
                        create_timestamp,
                        serialized_key_size,
                        serialized_value_size,
                    ),
                    accepted,
                )| {
                    let diagnostic = accepted.fault().map(translate_accepted_fault);
                    ProducerDelivery::new(
                        topic,
                        topic_uuid,
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
