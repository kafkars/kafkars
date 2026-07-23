//! Public immediate-admission handle over one synchronized producer shard.

use std::sync::Arc;

use super::super::ingress::ProducerAdmissionPort;
use super::super::ingress::ProducerWaitingStart;
use super::{
    capture::{
        ProducerSendCapture, ProducerSendCaptureError, ProducerSendCaptureErrorKind,
        ProducerSendOptions,
    },
    error::{ProducerTrySendError, ProducerTrySendErrorKind},
    prepare::prepare_explicit,
    record::ProducerRecord,
    result::ProducerTrySendAccepted,
    send::ProducerSend,
    send_error::{ready_from_pending_rejection, ready_from_try_send_kind},
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
        let prepared = prepare_explicit(capture, record)?;
        let (attempted_at, deadline, stored) = prepared.into_parts();
        match self.port.try_admit_explicit(attempted_at, deadline, stored) {
            Ok(accepted) => Ok(ProducerTrySendAccepted::from_port(
                accepted,
                deadline.transport(),
            )),
            Err(error) => Err(ProducerTrySendError::from_port(error)),
        }
    }

    /// Starts the engine-internal waiting path without publishing it to adapters.
    pub(crate) fn send(
        &self,
        record: ProducerRecord,
        options: ProducerSendOptions,
    ) -> ProducerSend {
        let capture = match self.capture_send(options) {
            Ok(capture) => capture,
            Err(error) => {
                drop(record);
                return ProducerSend::from_ready(ready_from_try_send_kind(capture_error_kind(
                    error.kind(),
                )));
            }
        };
        self.send_captured(capture, record)
    }

    /// Consumes one captured boundary into immediate or bounded pending ownership.
    pub(crate) fn send_captured(
        &self,
        capture: ProducerSendCapture,
        record: ProducerRecord,
    ) -> ProducerSend {
        let prepared = match prepare_explicit(capture, record) {
            Ok(prepared) => prepared,
            Err(error) => {
                let kind = error.kind();
                drop(error);
                return ProducerSend::from_ready(ready_from_try_send_kind(kind));
            }
        };
        let (attempted_at, deadline, stored) = prepared.into_parts();
        match self
            .port
            .start_waiting_explicit(attempted_at, deadline, stored)
        {
            ProducerWaitingStart::Accepted(accepted) => {
                let (observer, _operation_id, _fault) = accepted.into_parts();
                ProducerSend::from_accepted(observer)
            }
            ProducerWaitingStart::Pending(registration) => registration.into_send(),
            ProducerWaitingStart::ImmediateFailure(error) => {
                let error = ProducerTrySendError::from_port(error);
                let kind = error.kind();
                drop(error);
                ProducerSend::from_ready(ready_from_try_send_kind(kind))
            }
            ProducerWaitingStart::PendingRejected(rejected) => {
                let reason = rejected.reason();
                drop(rejected.into_record());
                ProducerSend::from_ready(ready_from_pending_rejection(reason))
            }
        }
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
