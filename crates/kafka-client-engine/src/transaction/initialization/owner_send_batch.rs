//! Linear public admission for one homogeneous transactional record batch.

use std::{fmt, sync::Arc, time::Duration};

use kafka_client_core::PartitionIndex;

use crate::{
    producer::{ProducerSendCapture, PublicProducerRecord, materialization::MaterializationRecord},
    transaction::send::TransactionSendInput,
};

use super::{
    TransactionBatchSendAdmissionError, TransactionBatchSendObserver,
    TransactionLifecycleControlAccepted, TransactionSendAdmissionErrorKind,
    TransactionSendObserver, TransactionToken,
    send_admission::{capture_error_kind, control_send_error_kind, record_error_kind},
};

impl<'owner> TransactionToken<'owner> {
    /// Returns the configured maximum records in one transactional Produce batch.
    pub fn batch_record_capacity(&self) -> usize {
        self.owner.transaction_batch_record_capacity()
    }

    /// Admits one nonempty same-topic, same-explicit-partition batch.
    pub fn send_batch<'send>(
        &'send mut self,
        records: Vec<PublicProducerRecord>,
        timeout: Duration,
    ) -> Result<TransactionBatchSendAccepted<'send, 'owner>, TransactionBatchSendAdmissionError>
    {
        let capture = match self.capture_send(timeout) {
            Ok(capture) => capture,
            Err(error) => {
                return Err(TransactionBatchSendAdmissionError::new(
                    capture_error_kind(error),
                    records,
                ));
            }
        };
        self.send_batch_captured(records, capture)
    }

    /// Admits one homogeneous batch under an already captured public boundary.
    pub fn send_batch_captured<'send>(
        &'send mut self,
        records: Vec<PublicProducerRecord>,
        capture: ProducerSendCapture,
    ) -> Result<TransactionBatchSendAccepted<'send, 'owner>, TransactionBatchSendAdmissionError>
    {
        let record_count = records.len();
        if record_count == 0 {
            return Err(TransactionBatchSendAdmissionError::new(
                TransactionSendAdmissionErrorKind::EmptyBatch,
                records,
            ));
        }
        let capacity = self.batch_record_capacity();
        if record_count > capacity {
            return Err(TransactionBatchSendAdmissionError::new(
                TransactionSendAdmissionErrorKind::BatchRecordCapacity {
                    actual: record_count,
                    limit: capacity,
                },
                records,
            ));
        }
        let (deadline, default_timestamp_ms) = capture.into_parts();
        let prepared = prepare_homogeneous(records, default_timestamp_ms)?;
        let observer_topic = Arc::clone(&prepared.topic);
        let public_partition = match i32::try_from(prepared.partition.get()) {
            Ok(partition) => partition,
            Err(_error) => {
                return Err(TransactionBatchSendAdmissionError::new(
                    TransactionSendAdmissionErrorKind::InvalidPartition,
                    prepared.records,
                ));
            }
        };
        let input = TransactionSendInput::new_batch(
            self.epoch,
            prepared.records,
            prepared.topic,
            prepared.partition,
            prepared.materializations,
            prepared.retained_source_bytes,
            deadline.operation_deadline(),
        );
        let TransactionLifecycleControlAccepted { value, wake_failed } =
            self.owner.send(input).map_err(|error| {
                let kind = control_send_error_kind(&error);
                TransactionBatchSendAdmissionError::new(
                    kind,
                    error.into_input().into_original_records(),
                )
            })?;
        let epoch = self.epoch;
        let send_id = value.send_id();
        let observer = TransactionSendObserver::new(
            value.into_observer(),
            self,
            epoch,
            send_id,
            observer_topic,
            Some(public_partition),
        );
        Ok(TransactionBatchSendAccepted {
            observer: TransactionBatchSendObserver::new(observer, record_count),
            wake_failed,
        })
    }
}

struct PreparedHomogeneousBatch {
    records: Vec<PublicProducerRecord>,
    topic: Arc<str>,
    partition: PartitionIndex,
    materializations: Vec<MaterializationRecord>,
    retained_source_bytes: usize,
}

fn prepare_homogeneous(
    records: Vec<PublicProducerRecord>,
    default_timestamp_ms: i64,
) -> Result<PreparedHomogeneousBatch, TransactionBatchSendAdmissionError> {
    let mut materializations = Vec::new();
    if materializations.try_reserve_exact(records.len()).is_err() {
        return Err(TransactionBatchSendAdmissionError::new(
            TransactionSendAdmissionErrorKind::Allocation,
            records,
        ));
    }
    let mut topic: Option<Arc<str>> = None;
    let mut partition = None;
    let mut retained_source_bytes = 0usize;
    for index in 0..records.len() {
        let view = match records[index].transaction_view(default_timestamp_ms) {
            Ok(view) => view,
            Err(error) => {
                return Err(TransactionBatchSendAdmissionError::new(
                    record_error_kind(error),
                    records,
                ));
            }
        };
        let (candidate_topic, candidate_partition, materialization, retained_bytes) =
            view.into_parts();
        let Some(candidate_partition) = candidate_partition else {
            return Err(TransactionBatchSendAdmissionError::new(
                TransactionSendAdmissionErrorKind::MissingExplicitPartition,
                records,
            ));
        };
        if topic
            .as_ref()
            .is_some_and(|topic| topic.as_ref() != candidate_topic.as_ref())
        {
            return Err(TransactionBatchSendAdmissionError::new(
                TransactionSendAdmissionErrorKind::MixedBatchTopic,
                records,
            ));
        }
        if partition.is_some_and(|partition| partition != candidate_partition) {
            return Err(TransactionBatchSendAdmissionError::new(
                TransactionSendAdmissionErrorKind::MixedBatchPartition,
                records,
            ));
        }
        retained_source_bytes = match retained_source_bytes.checked_add(retained_bytes) {
            Some(bytes) => bytes,
            None => {
                return Err(TransactionBatchSendAdmissionError::new(
                    TransactionSendAdmissionErrorKind::RetainedSizeOverflow,
                    records,
                ));
            }
        };
        topic.get_or_insert(candidate_topic);
        partition.get_or_insert(candidate_partition);
        materializations.push(materialization);
    }
    Ok(PreparedHomogeneousBatch {
        records,
        topic: topic.unwrap_or_else(|| unreachable!("nonempty batch has one topic")),
        partition: partition.unwrap_or_else(|| unreachable!("validated batch has one partition")),
        materializations,
        retained_source_bytes,
    })
}

/// Accepted homogeneous batch ownership plus advisory post-admission wake status.
#[must_use = "accepted transactional batch retains its sole terminal observer"]
pub struct TransactionBatchSendAccepted<'send, 'owner> {
    observer: TransactionBatchSendObserver<'send, 'owner>,
    wake_failed: bool,
}

impl<'send, 'owner> TransactionBatchSendAccepted<'send, 'owner> {
    /// Reports that the advisory reactor wake failed after batch acceptance.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole runtime-neutral batch observer.
    pub fn into_observer(self) -> TransactionBatchSendObserver<'send, 'owner> {
        self.observer
    }
}

impl fmt::Debug for TransactionBatchSendAccepted<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionBatchSendAccepted")
            .field("observer", &self.observer)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}
