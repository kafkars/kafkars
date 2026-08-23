//! Lossless facade admission of one hosted share-member registration.

use std::{sync::Arc, time::Duration};

use kafka_client_engine::{
    Engine,
    share::{
        ShareConsumerHandle as EngineShareConsumerHandle,
        ShareConsumerRegistration as EngineShareConsumerRegistration,
        ShareConsumerRegistrationErrorKind, ShareConsumerStartCapture,
    },
};

use crate::{ErrorKind, KafkaError};

/// Private unique bridge retaining one hosted share member.
pub(crate) struct ShareConsumerEngine {
    pub(super) handle: EngineShareConsumerHandle,
    pub(super) startup_fault: Option<KafkaError>,
}

impl ShareConsumerEngine {
    pub(crate) fn register(
        engine: &Engine,
        capture: ShareConsumerStartCapture,
        group: &str,
        rack: Option<&str>,
        topics: &[String],
        close_timeout: Duration,
    ) -> Result<Self, KafkaError> {
        let mut registration = EngineShareConsumerRegistration::new(
            Arc::<str>::from(group),
            topics
                .iter()
                .map(|topic| Arc::<str>::from(topic.as_str()))
                .collect(),
        )
        .with_close_timeout(close_timeout);
        if let Some(rack) = rack {
            registration = registration.with_rack(Arc::<str>::from(rack));
        }
        let handle = engine
            .register_share_consumer(capture, registration)
            .map_err(|error| {
                let semantic = translate_registration_kind(error.kind());
                drop(error.into_registration());
                semantic
            })?;
        let startup_fault = handle.startup_wake_failed().then(|| {
            KafkaError::new(
                ErrorKind::Internal,
                "share membership was accepted but host wakeup failed",
            )
        });
        Ok(Self {
            handle,
            startup_fault,
        })
    }
}

pub(crate) fn translate_registration_kind(kind: ShareConsumerRegistrationErrorKind) -> KafkaError {
    match kind {
        ShareConsumerRegistrationErrorKind::Closed => {
            KafkaError::new(ErrorKind::State, "share-consumer registration is closed")
        }
        ShareConsumerRegistrationErrorKind::Contended => KafkaError::new(
            ErrorKind::Backpressure,
            "share-consumer registration is temporarily contended",
        )
        .with_safe_retry(),
        ShareConsumerRegistrationErrorKind::Backpressure => KafkaError::new(
            ErrorKind::Backpressure,
            "bounded share-consumer registration capacity is full",
        )
        .with_safe_retry(),
        ShareConsumerRegistrationErrorKind::InvalidInput => KafkaError::new(
            ErrorKind::Configuration,
            "share-consumer registration is outside the supported bounded domain",
        ),
        ShareConsumerRegistrationErrorKind::Internal => KafkaError::new(
            ErrorKind::Internal,
            "share-consumer registration ownership is unavailable",
        ),
    }
}

impl core::fmt::Debug for ShareConsumerEngine {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ShareConsumerEngine")
            .field("handle", &self.handle)
            .field("startup_fault", &self.startup_fault)
            .finish_non_exhaustive()
    }
}
