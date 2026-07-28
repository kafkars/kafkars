//! Public immediate-admission handle over one synchronized producer shard.

mod waiting;

use std::sync::Arc;

use super::super::ingress::ProducerAdmissionPort;
use super::{
    batch_result::{ProducerTrySendBatch, ProducerTrySendBatchError},
    capture::{
        ProducerBatchSendCapture, ProducerSendCapture, ProducerSendCaptureError,
        ProducerSendCaptureErrorKind, ProducerSendOptions,
    },
    close::{ProducerTryCloseAccepted, ProducerTryCloseError},
    error::{ProducerTrySendError, ProducerTrySendErrorKind},
    flush_error::ProducerTryFlushError,
    flush_result::ProducerTryFlushAccepted,
    prepare::{prepare_batch, prepare_explicit, prepare_waiting},
    record::ProducerRecord,
    result::ProducerTrySendAccepted,
};
use crate::clock::MonotonicClock;

/// Cloneable, runtime-neutral producer admission handle.
#[derive(Clone)]
pub struct ProducerHandle {
    port: ProducerAdmissionPort,
    clock: Arc<MonotonicClock>,
    batch_admission_capacity: usize,
    lifetime: Arc<dyn Send + Sync>,
}

impl ProducerHandle {
    pub(crate) fn from_port(
        port: ProducerAdmissionPort,
        clock: Arc<MonotonicClock>,
        batch_admission_capacity: usize,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            port,
            clock,
            batch_admission_capacity,
            lifetime,
        }
    }

    /// Returns the maximum caller-owned record vector accepted by one batch call.
    pub const fn batch_admission_capacity(&self) -> usize {
        self.batch_admission_capacity
    }

    /// Captures time before a Rust facade converts its record.
    pub fn capture_send(
        &self,
        options: ProducerSendOptions,
    ) -> Result<ProducerSendCapture, ProducerSendCaptureError> {
        ProducerSendCapture::capture(&self.clock, options)
    }

    /// Captures one boundary shared by every record in a batch call.
    pub fn capture_batch(
        &self,
        options: ProducerSendOptions,
    ) -> Result<ProducerBatchSendCapture, ProducerSendCaptureError> {
        ProducerBatchSendCapture::capture(&self.clock, options)
    }

    /// Attempts one atomic explicit or automatic-partition admission without blocking.
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
        let explicit = record.explicit_partition().is_some();
        let prepared = if explicit {
            prepare_explicit(capture, record)
        } else {
            prepare_waiting(capture, record)
        }?;
        let (attempted_at, deadline, stored) = prepared.into_parts();
        let admission = if explicit {
            self.port.try_admit_explicit(attempted_at, deadline, stored)
        } else {
            self.port.try_admit_waiting(attempted_at, deadline, stored)
        };
        match admission {
            Ok(accepted) => Ok(ProducerTrySendAccepted::from_port(accepted)),
            Err(error) => Err(ProducerTrySendError::from_port(error)),
        }
    }

    /// Admits one ordered explicit-or-automatic prefix under one shard lock.
    ///
    /// Validation precedes any admission. After validation, the first bounded
    /// admission rejection stops the call; the exact rejected record and
    /// untouched suffix remain in the returned batch error.
    pub fn try_send_batch_captured(
        &self,
        capture: ProducerBatchSendCapture,
        records: Vec<ProducerRecord>,
    ) -> ProducerTrySendBatch {
        let (attempted_at, deadline, records) = match prepare_batch(capture, records) {
            Ok(prepared) => prepared.into_parts(),
            Err(rejected) => {
                let (kind, records) = rejected.into_parts();
                return ProducerTrySendBatch::new(
                    Vec::new(),
                    Some(ProducerTrySendBatchError::from_parts(kind, records, None)),
                );
            }
        };
        let admitted = self.port.try_admit_batch(attempted_at, deadline, records);
        let (accepted, rejection) = admitted.into_parts();
        let accepted = accepted
            .into_iter()
            .map(ProducerTrySendAccepted::from_port)
            .collect();
        let rejection = rejection.map(|rejection| {
            let (first, remaining) = rejection.into_parts();
            let remaining = remaining
                .into_iter()
                .map(ProducerRecord::from_stored)
                .collect();
            ProducerTrySendBatchError::from_single(
                ProducerTrySendError::from_port(first),
                remaining,
            )
        });
        ProducerTrySendBatch::new(accepted, rejection)
    }

    /// Attempts one producer flush admission without blocking.
    pub fn try_flush(&self) -> Result<ProducerTryFlushAccepted, ProducerTryFlushError> {
        let now = self
            .clock
            .now()
            .map_err(|_error| ProducerTryFlushError::moment_unrepresentable())?;
        self.port
            .try_admit_flush(now)
            .map(ProducerTryFlushAccepted::from_port)
            .map_err(ProducerTryFlushError::from_port)
    }

    /// Atomically closes record admission and accepts one drain barrier.
    pub fn try_close(&self) -> Result<ProducerTryCloseAccepted, ProducerTryCloseError> {
        let now = self
            .clock
            .now()
            .map_err(|_error| ProducerTryCloseError::moment_unrepresentable())?;
        self.port
            .try_admit_close(now)
            .map(ProducerTryCloseAccepted::from_port)
            .map_err(ProducerTryCloseError::from_port)
    }

    #[cfg(test)]
    pub(crate) fn shard_stats(&self) -> crate::producer::ingress::ProducerShardStats {
        self.port
            .shard_stats()
            .unwrap_or_else(|error| panic!("producer shard stats lock failed: {error:?}"))
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
            .field("batch_admission_capacity", &self.batch_admission_capacity)
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
