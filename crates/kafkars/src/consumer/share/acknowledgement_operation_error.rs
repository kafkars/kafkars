//! Public lossless admission and terminal errors for share acknowledgements.

use crate::{KafkaError, bridge::share_consumer};

use super::ShareAcknowledgement;

/// Pre-admission rejection retaining the exact acknowledgement capability.
#[must_use = "acknowledgement rejection retains the exact linear capability"]
pub struct ShareAcknowledgementAdmissionError {
    acknowledgement: ShareAcknowledgement,
    error: KafkaError,
}

impl ShareAcknowledgementAdmissionError {
    pub(super) const fn new(acknowledgement: ShareAcknowledgement, error: KafkaError) -> Self {
        Self {
            acknowledgement,
            error,
        }
    }

    /// Borrows the exact acknowledgement that did not transfer.
    pub const fn acknowledgement(&self) -> &ShareAcknowledgement {
        &self.acknowledgement
    }

    /// Borrows the stable semantic rejection.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Recovers the exact acknowledgement and stable semantic rejection.
    pub fn into_parts(self) -> (ShareAcknowledgement, KafkaError) {
        (self.acknowledgement, self.error)
    }
}

impl std::fmt::Debug for ShareAcknowledgementAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementAdmissionError")
            .field("acknowledgement", &self.acknowledgement)
            .field("error", &self.error)
            .finish()
    }
}

impl std::fmt::Display for ShareAcknowledgementAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ShareAcknowledgementAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Exact top-level broker rejection details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShareAcknowledgementBrokerError {
    throttle_time_ms: u32,
    broker_code: i16,
    message: Option<Vec<u8>>,
}

impl ShareAcknowledgementBrokerError {
    pub(super) fn from_bridge(inner: &share_consumer::ShareAcknowledgementBrokerError) -> Self {
        Self {
            throttle_time_ms: inner.throttle_time_ms(),
            broker_code: inner.broker_code(),
            message: inner.message().map(<[u8]>::to_vec),
        }
    }

    /// Returns Kafka's nonnegative response throttle in milliseconds.
    pub const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    /// Returns Kafka's exact nonzero top-level error code.
    pub const fn broker_code(&self) -> i16 {
        self.broker_code
    }

    /// Returns Kafka's bounded diagnostic bytes without UTF-8 coercion.
    pub fn message(&self) -> Option<&[u8]> {
        self.message.as_deref()
    }
}

/// Accepted-operation failure with exact retry ownership only when safe.
#[must_use = "share acknowledgement failure may retain exact retry ownership"]
pub struct ShareAcknowledgementError {
    acknowledgement: Option<Box<ShareAcknowledgement>>,
    error: KafkaError,
    broker: Option<ShareAcknowledgementBrokerError>,
}

impl ShareAcknowledgementError {
    pub(super) fn from_bridge(inner: share_consumer::ShareAcknowledgementError) -> Self {
        let broker = inner
            .broker()
            .map(ShareAcknowledgementBrokerError::from_bridge);
        let (acknowledgement, error) = inner.into_parts();
        Self {
            acknowledgement: acknowledgement
                .map(ShareAcknowledgement::from_bridge)
                .map(Box::new),
            error,
            broker,
        }
    }

    /// Borrows the stable semantic terminal failure.
    pub const fn error(&self) -> &KafkaError {
        &self.error
    }

    /// Borrows the exact definitely-unsent acknowledgement retained for retry.
    pub fn acknowledgement(&self) -> Option<&ShareAcknowledgement> {
        self.acknowledgement.as_deref()
    }

    /// Borrows the exact top-level broker rejection when available.
    pub const fn broker(&self) -> Option<&ShareAcknowledgementBrokerError> {
        self.broker.as_ref()
    }

    /// Returns safe retry ownership, the semantic error, and broker details.
    pub fn into_parts(
        self,
    ) -> (
        Option<ShareAcknowledgement>,
        KafkaError,
        Option<ShareAcknowledgementBrokerError>,
    ) {
        (
            self.acknowledgement.map(|acknowledgement| *acknowledgement),
            self.error,
            self.broker,
        )
    }
}

impl std::fmt::Debug for ShareAcknowledgementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgementError")
            .field("acknowledgement", &self.acknowledgement)
            .field("error", &self.error)
            .field("broker", &self.broker)
            .finish()
    }
}

impl std::fmt::Display for ShareAcknowledgementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ShareAcknowledgementError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
