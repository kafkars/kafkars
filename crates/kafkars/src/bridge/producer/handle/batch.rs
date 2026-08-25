//! Atomic producer-batch admission and per-record delivery reconstruction.

use kafka_client_engine::{ProducerTrySendAccepted, ProducerTrySendBatchError};

use super::ProducerEngine;
use crate::{
    Record,
    bridge::{
        producer::{
            batch::ProducerBatch, conversion::validate_batch_records, delivery::ProducerDelivery,
            prepare_engine_records,
        },
        producer_result::admission::{
            conversion_allocation_error, translate_accepted_fault, translate_batch_admission_error,
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
        let mut deliveries = Vec::new();
        if deliveries.try_reserve_exact(records.len()).is_err() {
            return ProducerBatch::new(
                deliveries,
                Some(crate::TrySendError::new(
                    records,
                    conversion_allocation_error(),
                )),
            );
        }
        let prepared = match prepare_engine_records(records) {
            Ok(prepared) => prepared,
            Err(records) => {
                return ProducerBatch::new(
                    deliveries,
                    Some(crate::TrySendError::new(
                        records,
                        conversion_allocation_error(),
                    )),
                );
            }
        };
        let default_timestamp = capture.default_timestamp_milliseconds();
        let (records, engine_records) = prepared.into_parts();
        let outcome = admission.try_send_captured(capture, engine_records);
        let (accepted, rejection) = outcome.into_parts();
        let accepted_count = accepted.len();
        append_deliveries(&mut deliveries, &records, accepted, default_timestamp);
        let rejection = reconcile_rejection(records, accepted_count, rejection);
        ProducerBatch::new(deliveries, rejection)
    }
}

fn append_deliveries(
    deliveries: &mut Vec<ProducerDelivery>,
    records: &[Record],
    accepted: Vec<ProducerTrySendAccepted>,
    default_timestamp: i64,
) {
    let accepted_originals = records
        .get(..accepted.len())
        .unwrap_or_else(|| unreachable!("engine accepted beyond the supplied batch"));
    for (record, accepted) in accepted_originals.iter().zip(accepted) {
        let diagnostic = accepted.fault().map(translate_accepted_fault);
        deliveries.push(ProducerDelivery::new(
            std::sync::Arc::clone(record.topic_owner()),
            record.expected_topic_uuid_value(),
            record.timestamp().unwrap_or(default_timestamp),
            record.key_bytes().map(bytes::Bytes::len),
            record.value_bytes().map(bytes::Bytes::len),
            accepted.into_observer(),
            diagnostic,
        ));
    }
}

fn reconcile_rejection(
    mut records: Vec<Record>,
    accepted_count: usize,
    rejection: Option<ProducerTrySendBatchError>,
) -> Option<crate::TrySendError<Vec<Record>>> {
    let Some(error) = rejection else {
        debug_assert_eq!(accepted_count, records.len());
        return None;
    };
    let (kind, engine_records, detail) = error.into_parts();
    debug_assert_eq!(
        engine_records.len(),
        records.len().saturating_sub(accepted_count),
        "engine rejection must own the original suffix"
    );
    drop(engine_records);
    drop(records.drain(..accepted_count));
    Some(crate::TrySendError::new(
        records,
        translate_batch_admission_error(kind, detail.as_deref()),
    ))
}
