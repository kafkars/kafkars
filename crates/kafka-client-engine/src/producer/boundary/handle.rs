//! Public immediate-admission handle over one synchronized producer shard.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::super::ingress::ProducerAdmissionPort;
use super::{
    error::{ProducerTrySendError, ProducerTrySendErrorKind},
    record::ProducerRecord,
    result::ProducerTrySendAccepted,
};
use crate::clock::MonotonicClock;

/// Per-call producer admission options captured before validation or locking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerSendOptions {
    delivery_timeout: Duration,
}

impl ProducerSendOptions {
    /// Creates options with one end-to-end delivery timeout.
    pub const fn new(delivery_timeout: Duration) -> Self {
        Self { delivery_timeout }
    }

    /// Returns the end-to-end delivery timeout.
    pub const fn delivery_timeout(self) -> Duration {
        self.delivery_timeout
    }
}

/// Cloneable, runtime-neutral producer admission handle.
#[derive(Clone)]
pub struct ProducerHandle {
    port: ProducerAdmissionPort,
    clock: Arc<MonotonicClock>,
}

impl ProducerHandle {
    pub(crate) fn from_port(port: ProducerAdmissionPort, clock: Arc<MonotonicClock>) -> Self {
        Self { port, clock }
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
        let Ok(capture) = self
            .clock
            .capture_deadline_after(options.delivery_timeout())
        else {
            return Err(ProducerTrySendError::with_record(
                ProducerTrySendErrorKind::DeadlineUnrepresentable,
                record,
            ));
        };
        let Some(default_timestamp_ms) = unix_timestamp_milliseconds() else {
            return Err(ProducerTrySendError::with_record(
                ProducerTrySendErrorKind::TimestampUnrepresentable,
                record,
            ));
        };
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
            .try_admit_explicit(capture.now(), capture.deadline(), stored)
        {
            Ok(accepted) => Ok(ProducerTrySendAccepted::from_port(
                accepted,
                capture.absolute_instant(),
            )),
            Err(error) => Err(ProducerTrySendError::from_port(error)),
        }
    }
}

impl std::fmt::Debug for ProducerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerHandle")
            .finish_non_exhaustive()
    }
}

fn unix_timestamp_milliseconds() -> Option<i64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(elapsed.as_millis()).ok()
}
