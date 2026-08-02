//! Private admission and completion translation for one metrics snapshot.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use kafka_client_engine::{
    EngineMetricsAdmissionErrorKind, EngineMetricsObserver, EngineMetricsObserverError,
    EngineMetricsObserverErrorKind,
};

use crate::{ErrorKind, KafkaError, bridge::ClientEngine};

use super::ClientMetricsSnapshot;

pub(crate) struct ClientMetricsObserver {
    inner: EngineMetricsObserver,
}

impl ClientEngine {
    /// Immediately admits one bounded operational snapshot with staged capture.
    pub(crate) fn metrics(&self) -> Result<ClientMetricsObserver, KafkaError> {
        self.inner
            .metrics()
            .map(ClientMetricsObserver::new)
            .map_err(|error| {
                let kind = match error.kind() {
                    EngineMetricsAdmissionErrorKind::Capacity => ErrorKind::Backpressure,
                    EngineMetricsAdmissionErrorKind::Closed => ErrorKind::State,
                    EngineMetricsAdmissionErrorKind::HostUnavailable => ErrorKind::Internal,
                };
                KafkaError::new(kind, error.to_string())
            })
    }
}

impl ClientMetricsObserver {
    const fn new(inner: EngineMetricsObserver) -> Self {
        Self { inner }
    }

    pub(crate) fn wait(self) -> Result<ClientMetricsSnapshot, KafkaError> {
        self.inner
            .wait()
            .map(ClientMetricsSnapshot::from_engine)
            .map_err(translate_error)
    }
}

impl Future for ClientMetricsObserver {
    type Output = Result<ClientMetricsSnapshot, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(context)
            .map(|result| {
                result
                    .map(ClientMetricsSnapshot::from_engine)
                    .map_err(translate_error)
            })
    }
}

fn translate_error(error: EngineMetricsObserverError) -> KafkaError {
    let kind = match error.kind() {
        EngineMetricsObserverErrorKind::Draining => ErrorKind::State,
        EngineMetricsObserverErrorKind::Completion => ErrorKind::Internal,
    };
    KafkaError::new(kind, error.to_string())
}
