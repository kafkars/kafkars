//! Public immediate-admission handle over one synchronized producer shard.

use std::sync::Arc;

use super::super::ingress::ProducerAdmissionPort;
use super::{
    capture::{
        ProducerSendCapture, ProducerSendCaptureError, ProducerSendCaptureErrorKind,
        ProducerSendOptions,
    },
    error::{ProducerTrySendError, ProducerTrySendErrorKind},
    record::ProducerRecord,
    result::ProducerTrySendAccepted,
};
use crate::clock::MonotonicClock;

/// Cloneable, runtime-neutral producer admission handle.
#[derive(Clone)]
pub struct ProducerHandle {
    port: ProducerAdmissionPort,
    clock: Arc<MonotonicClock>,
    lifetime: Arc<dyn Send + Sync>,
}

impl ProducerHandle {
    pub(crate) fn from_port(
        port: ProducerAdmissionPort,
        clock: Arc<MonotonicClock>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            port,
            clock,
            lifetime,
        }
    }

    /// Captures time before a Rust facade converts its record.
    pub fn capture_send(
        &self,
        options: ProducerSendOptions,
    ) -> Result<ProducerSendCapture, ProducerSendCaptureError> {
        ProducerSendCapture::capture(&self.clock, options)
    }

    /// Attempts one atomic explicit-partition admission without waiting.
    ///
    /// The monotonic deadline is captured before record validation, timestamp
    /// defaulting, or shard locking. Missing timestamps use Unix epoch
    /// milliseconds sampled by the engine at this call boundary.
    #[allow(
        clippy::result_large_err,
        reason = "pre-admission failures return the intact bytes-native record"
    )]
    pub fn try_send(
        &self,
        record: ProducerRecord,
        options: ProducerSendOptions,
    ) -> Result<ProducerTrySendAccepted, ProducerTrySendError> {
        let capture = match self.capture_send(options) {
            Ok(capture) => capture,
            Err(error) => {
                return Err(ProducerTrySendError::with_record(
                    capture_error_kind(error.kind()),
                    record,
                ));
            }
        };
        self.try_send_captured(capture, record)
    }

    /// Consumes one original call boundary after adapter-owned record conversion.
    #[allow(
        clippy::result_large_err,
        reason = "pre-admission failures return the intact bytes-native record"
    )]
    pub fn try_send_captured(
        &self,
        capture: ProducerSendCapture,
        record: ProducerRecord,
    ) -> Result<ProducerTrySendAccepted, ProducerTrySendError> {
        let (deadline, default_timestamp_ms) = capture.into_parts();
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
        let stored = record.into_stored(partition, default_timestamp_ms);
        match self
            .port
            .try_admit_explicit(deadline.now(), deadline.operation_deadline(), stored)
        {
            Ok(accepted) => Ok(ProducerTrySendAccepted::from_port(
                accepted,
                deadline.operation_deadline().transport(),
            )),
            Err(error) => Err(ProducerTrySendError::from_port(error)),
        }
    }

    #[cfg(test)]
    pub(crate) fn host_stats(&self) -> crate::producer::host::ProducerHostStats {
        self.port
            .host_stats()
            .unwrap_or_else(|error| panic!("producer host stats lock failed: {error:?}"))
    }

    #[cfg(test)]
    pub(crate) fn inject_terminal_interpretation_fault(&self) {
        self.port
            .inject_terminal_interpretation_fault()
            .unwrap_or_else(|error| panic!("producer host fault injection failed: {error:?}"));
    }
}

impl std::fmt::Debug for ProducerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerHandle")
            .field("host_retained", &Arc::strong_count(&self.lifetime))
            .finish_non_exhaustive()
    }
}

const fn capture_error_kind(kind: ProducerSendCaptureErrorKind) -> ProducerTrySendErrorKind {
    match kind {
        ProducerSendCaptureErrorKind::DeadlineUnrepresentable => {
            ProducerTrySendErrorKind::DeadlineUnrepresentable
        }
        ProducerSendCaptureErrorKind::TimestampUnrepresentable => {
            ProducerTrySendErrorKind::TimestampUnrepresentable
        }
    }
}
