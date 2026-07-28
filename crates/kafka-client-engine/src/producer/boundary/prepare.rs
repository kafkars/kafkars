//! One-time record preparation for immediate and waiting producer admission.

use kafka_client_core::Moment;

use super::{
    ProducerBatchSendCapture, ProducerSendCapture, ProducerTrySendError, ProducerTrySendErrorKind,
    record::ProducerRecord as PublicProducerRecord,
};
use crate::{clock::OperationDeadline, producer::ProducerRecord};

/// Validated engine record paired with its one original public boundary.
pub(super) struct PreparedExplicitSend {
    attempted_at: Moment,
    deadline: OperationDeadline,
    record: ProducerRecord,
}

/// Fully validated engine-owned batch beside its one captured boundary.
pub(super) struct PreparedBatch {
    attempted_at: Moment,
    deadline: OperationDeadline,
    records: Vec<ProducerRecord>,
}

impl PreparedBatch {
    pub(super) fn into_parts(self) -> (Moment, OperationDeadline, Vec<ProducerRecord>) {
        (self.attempted_at, self.deadline, self.records)
    }
}

/// Validation rejection retaining the complete original engine-boundary batch.
pub(super) struct RejectedBatch {
    kind: ProducerTrySendErrorKind,
    records: Vec<PublicProducerRecord>,
}

impl RejectedBatch {
    pub(super) fn into_parts(self) -> (ProducerTrySendErrorKind, Vec<PublicProducerRecord>) {
        (self.kind, self.records)
    }
}

impl PreparedExplicitSend {
    pub(super) fn into_parts(self) -> (Moment, OperationDeadline, ProducerRecord) {
        (self.attempted_at, self.deadline, self.record)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "pre-admission validation returns the intact bytes-native record"
)]
pub(super) fn prepare_explicit(
    capture: ProducerSendCapture,
    record: PublicProducerRecord,
) -> Result<PreparedExplicitSend, ProducerTrySendError> {
    let (capture, default_timestamp_ms) = capture.into_parts();
    if record.topic().is_empty() {
        return Err(ProducerTrySendError::with_record(
            ProducerTrySendErrorKind::EmptyTopic,
            record,
        ));
    }
    let partition = match validate_explicit(&record) {
        Ok(partition) => partition,
        Err(kind) => return Err(ProducerTrySendError::with_record(kind, record)),
    };
    Ok(PreparedExplicitSend {
        attempted_at: capture.now(),
        deadline: capture.operation_deadline(),
        record: record.into_stored(Some(partition), default_timestamp_ms),
    })
}

#[allow(
    clippy::result_large_err,
    reason = "pre-admission validation returns the intact bytes-native record"
)]
pub(super) fn prepare_waiting(
    capture: ProducerSendCapture,
    record: PublicProducerRecord,
) -> Result<PreparedExplicitSend, ProducerTrySendError> {
    let (capture, default_timestamp_ms) = capture.into_parts();
    let partition = match validate_optional_partition(&record) {
        Ok(partition) => partition,
        Err(kind) => return Err(ProducerTrySendError::with_record(kind, record)),
    };
    Ok(PreparedExplicitSend {
        attempted_at: capture.now(),
        deadline: capture.operation_deadline(),
        record: record.into_stored(partition, default_timestamp_ms),
    })
}

pub(super) fn prepare_batch(
    capture: ProducerBatchSendCapture,
    records: Vec<PublicProducerRecord>,
) -> Result<PreparedBatch, RejectedBatch> {
    let (capture, default_timestamp_ms) = capture.into_parts();
    for record in &records {
        if let Err(kind) = validate_optional_partition(record) {
            return Err(RejectedBatch { kind, records });
        }
    }
    let records = records
        .into_iter()
        .map(|record| {
            let partition = validate_optional_partition(&record)
                .unwrap_or_else(|_| unreachable!("complete batch validation already succeeded"));
            record.into_stored(partition, default_timestamp_ms)
        })
        .collect();
    Ok(PreparedBatch {
        attempted_at: capture.now(),
        deadline: capture.operation_deadline(),
        records,
    })
}

fn validate_explicit(
    record: &PublicProducerRecord,
) -> Result<kafka_client_core::PartitionIndex, ProducerTrySendErrorKind> {
    if record.topic().is_empty() {
        return Err(ProducerTrySendErrorKind::EmptyTopic);
    }
    match record.explicit_partition() {
        None => Err(ProducerTrySendErrorKind::MissingExplicitPartition),
        Some(partition) if partition < 0 => {
            Err(ProducerTrySendErrorKind::NegativeExplicitPartition)
        }
        Some(_) => record
            .validate_explicit_partition()
            .ok_or(ProducerTrySendErrorKind::NegativeExplicitPartition),
    }
}

fn validate_optional_partition(
    record: &PublicProducerRecord,
) -> Result<Option<kafka_client_core::PartitionIndex>, ProducerTrySendErrorKind> {
    if record.topic().is_empty() {
        return Err(ProducerTrySendErrorKind::EmptyTopic);
    }
    match record.explicit_partition() {
        None => Ok(None),
        Some(partition) if partition < 0 => {
            Err(ProducerTrySendErrorKind::NegativeExplicitPartition)
        }
        Some(_) => record
            .validate_explicit_partition()
            .map(Some)
            .ok_or(ProducerTrySendErrorKind::NegativeExplicitPartition),
    }
}
