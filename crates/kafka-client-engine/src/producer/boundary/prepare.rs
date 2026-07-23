//! One-time explicit-record preparation shared by immediate and waiting admission.

use kafka_client_core::Moment;

use super::{
    ProducerSendCapture, ProducerTrySendError, ProducerTrySendErrorKind,
    record::ProducerRecord as PublicProducerRecord,
};
use crate::{clock::OperationDeadline, producer::ProducerRecord};

/// Validated engine record paired with its one original public boundary.
pub(super) struct PreparedExplicitSend {
    attempted_at: Moment,
    deadline: OperationDeadline,
    record: ProducerRecord,
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
    let partition = match record.explicit_partition() {
        None => {
            return Err(ProducerTrySendError::with_record(
                ProducerTrySendErrorKind::MissingExplicitPartition,
                record,
            ));
        }
        Some(partition) if partition < 0 => {
            return Err(ProducerTrySendError::with_record(
                ProducerTrySendErrorKind::NegativeExplicitPartition,
                record,
            ));
        }
        Some(_) => match record.validate_explicit_partition() {
            Some(partition) => partition,
            None => {
                return Err(ProducerTrySendError::with_record(
                    ProducerTrySendErrorKind::NegativeExplicitPartition,
                    record,
                ));
            }
        },
    };
    Ok(PreparedExplicitSend {
        attempted_at: capture.now(),
        deadline: capture.operation_deadline(),
        record: record.into_stored(partition, default_timestamp_ms),
    })
}
