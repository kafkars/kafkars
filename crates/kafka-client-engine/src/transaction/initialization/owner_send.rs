//! Linear public transactional-record send admission.

use std::{fmt, sync::Arc, time::Duration};

use crate::{
    producer::{ProducerSendCapture, ProducerSendCaptureError, PublicProducerRecord},
    transaction::send::TransactionSendInput,
};

use super::{
    TransactionLifecycleControlAccepted, TransactionSendAdmissionError, TransactionSendObserver,
    TransactionToken,
    send_admission::{capture_error_kind, control_send_error_kind, record_error_kind},
};

impl<'owner> TransactionToken<'owner> {
    /// Captures one transactional send boundary before record conversion.
    pub fn capture_send(
        &self,
        timeout: Duration,
    ) -> Result<ProducerSendCapture, ProducerSendCaptureError> {
        self.owner.capture_send(timeout)
    }

    /// Admits one record into this exact transaction.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact caller-owned transactional record"
    )]
    pub fn send<'send>(
        &'send mut self,
        record: PublicProducerRecord,
        timeout: Duration,
    ) -> Result<TransactionSendAccepted<'send, 'owner>, TransactionSendAdmissionError> {
        let capture = match self.capture_send(timeout) {
            Ok(capture) => capture,
            Err(error) => {
                return Err(TransactionSendAdmissionError::new(
                    capture_error_kind(error),
                    record,
                ));
            }
        };
        self.send_captured(record, capture)
    }

    /// Admits one record under an already captured public send boundary.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact caller-owned transactional record"
    )]
    pub fn send_captured<'send>(
        &'send mut self,
        record: PublicProducerRecord,
        capture: ProducerSendCapture,
    ) -> Result<TransactionSendAccepted<'send, 'owner>, TransactionSendAdmissionError> {
        let (deadline, default_timestamp_ms) = capture.into_parts();
        let observer_topic_uuid = record.expected_topic_uuid_value();
        let view = match record.transaction_view(default_timestamp_ms) {
            Ok(view) => view,
            Err(error) => {
                return Err(TransactionSendAdmissionError::new(
                    record_error_kind(error),
                    record,
                ));
            }
        };
        let (topic, partition, materialization, retained_source_bytes) = view.into_parts();
        let public_partition = match partition
            .map(|partition| i32::try_from(partition.get()))
            .transpose()
        {
            Ok(partition) => partition,
            Err(_error) => {
                return Err(TransactionSendAdmissionError::new(
                    super::TransactionSendAdmissionErrorKind::InvalidPartition,
                    record,
                ));
            }
        };
        let observer_topic = Arc::clone(&topic);
        let input = match TransactionSendInput::try_new(
            self.epoch,
            record,
            topic,
            partition,
            materialization,
            retained_source_bytes,
            deadline.operation_deadline(),
        ) {
            Ok(input) => input,
            Err(record) => {
                return Err(TransactionSendAdmissionError::new(
                    super::TransactionSendAdmissionErrorKind::Allocation,
                    record,
                ));
            }
        };
        let TransactionLifecycleControlAccepted { value, wake_failed } =
            self.owner.send(input).map_err(|error| {
                let kind = control_send_error_kind(&error);
                TransactionSendAdmissionError::new(kind, error.into_input().into_original_record())
            })?;
        let epoch = self.epoch;
        let send_id = value.send_id();
        Ok(TransactionSendAccepted {
            observer: TransactionSendObserver::new(
                value.into_observer(),
                self,
                epoch,
                send_id,
                observer_topic,
                observer_topic_uuid,
                public_partition,
            ),
            wake_failed,
        })
    }
}

/// Accepted send ownership plus advisory post-admission wake status.
#[must_use = "accepted transactional send retains its sole terminal observer"]
pub struct TransactionSendAccepted<'send, 'owner> {
    observer: TransactionSendObserver<'send, 'owner>,
    wake_failed: bool,
}

impl<'send, 'owner> TransactionSendAccepted<'send, 'owner> {
    /// Reports that the advisory reactor wake failed after send acceptance.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole runtime-neutral send observer.
    pub fn into_observer(self) -> TransactionSendObserver<'send, 'owner> {
        self.observer
    }
}

impl fmt::Debug for TransactionSendAccepted<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransactionSendAccepted")
            .field("observer", &self.observer)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}
