//! Linear public-boundary time capture retained across record conversion.

use std::{
    fmt,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::clock::{DeadlineCapture, MonotonicClock};

/// Per-call producer admission options captured before conversion or validation.
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

/// Stable reason the engine could not capture a producer call boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerSendCaptureErrorKind {
    /// The monotonic deadline cannot be represented.
    DeadlineUnrepresentable,
    /// The boundary Unix timestamp cannot be represented.
    TimestampUnrepresentable,
}

/// Failure before producer record ownership crosses into engine conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerSendCaptureError {
    kind: ProducerSendCaptureErrorKind,
}

impl ProducerSendCaptureError {
    /// Returns the stable boundary-capture failure category.
    pub const fn kind(self) -> ProducerSendCaptureErrorKind {
        self.kind
    }

    const fn new(kind: ProducerSendCaptureErrorKind) -> Self {
        Self { kind }
    }
}

impl fmt::Display for ProducerSendCaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            ProducerSendCaptureErrorKind::DeadlineUnrepresentable => {
                "producer delivery deadline cannot be represented"
            }
            ProducerSendCaptureErrorKind::TimestampUnrepresentable => {
                "producer boundary timestamp cannot be represented"
            }
        })
    }
}

impl std::error::Error for ProducerSendCaptureError {}

/// One non-cloneable producer call boundary consumed by one admission attempt.
#[must_use = "a captured producer boundary must be consumed by one admission path"]
#[derive(Debug)]
pub struct ProducerSendCapture {
    deadline: DeadlineCapture,
    default_timestamp_ms: i64,
}

impl ProducerSendCapture {
    pub(super) fn capture(
        clock: &MonotonicClock,
        options: ProducerSendOptions,
    ) -> Result<Self, ProducerSendCaptureError> {
        let deadline = clock
            .capture_deadline_after(options.delivery_timeout())
            .map_err(|_error| {
                ProducerSendCaptureError::new(ProducerSendCaptureErrorKind::DeadlineUnrepresentable)
            })?;
        let Some(default_timestamp_ms) = unix_timestamp_milliseconds() else {
            return Err(ProducerSendCaptureError::new(
                ProducerSendCaptureErrorKind::TimestampUnrepresentable,
            ));
        };
        Ok(Self {
            deadline,
            default_timestamp_ms,
        })
    }

    pub(crate) fn capture_transaction(
        clock: &MonotonicClock,
        timeout: Duration,
    ) -> Result<Self, ProducerSendCaptureError> {
        if timeout.is_zero() {
            return Err(ProducerSendCaptureError::new(
                ProducerSendCaptureErrorKind::DeadlineUnrepresentable,
            ));
        }
        Self::capture(clock, ProducerSendOptions::new(timeout))
    }

    /// Returns the original absolute monotonic deadline reserved for driver handoff.
    pub const fn absolute_deadline(&self) -> Instant {
        self.deadline.operation_deadline().transport()
    }

    pub(crate) const fn into_parts(self) -> (DeadlineCapture, i64) {
        (self.deadline, self.default_timestamp_ms)
    }
}

/// One non-cloneable batch call boundary shared by every record in that call.
#[must_use = "a captured producer batch boundary must be consumed by one admission path"]
#[derive(Debug)]
pub struct ProducerBatchSendCapture {
    deadline: DeadlineCapture,
    default_timestamp_ms: i64,
}

impl ProducerBatchSendCapture {
    pub(super) fn capture(
        clock: &MonotonicClock,
        options: ProducerSendOptions,
    ) -> Result<Self, ProducerSendCaptureError> {
        let single = ProducerSendCapture::capture(clock, options)?;
        let (deadline, default_timestamp_ms) = single.into_parts();
        Ok(Self {
            deadline,
            default_timestamp_ms,
        })
    }

    /// Returns the one absolute monotonic deadline shared by the batch.
    pub const fn absolute_deadline(&self) -> Instant {
        self.deadline.operation_deadline().transport()
    }

    pub(super) const fn into_parts(self) -> (DeadlineCapture, i64) {
        (self.deadline, self.default_timestamp_ms)
    }
}

fn unix_timestamp_milliseconds() -> Option<i64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(elapsed.as_millis()).ok()
}
