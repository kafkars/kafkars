//! One facade-owned shutdown worker and bounded shared terminal observation.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use kafka_client_engine::Engine;

use crate::KafkaError;

mod state;
#[cfg(test)]
mod state_test;

use state::ShutdownShared;

#[derive(Clone)]
pub(crate) struct ClientShutdownOwner {
    shared: Arc<ShutdownShared>,
}

pub(crate) struct ClientShutdown {
    shared: Arc<ShutdownShared>,
    registration: Option<u64>,
}

impl ClientShutdownOwner {
    pub(crate) fn try_new(engine: Engine) -> Result<Self, KafkaError> {
        Ok(Self {
            shared: ShutdownShared::try_new(engine)?,
        })
    }

    pub(crate) fn begin(&self) -> ClientShutdown {
        self.shared.begin();
        ClientShutdown {
            shared: Arc::clone(&self.shared),
            registration: None,
        }
    }
}

impl ClientShutdown {
    pub(crate) fn wait(mut self) -> Result<(), KafkaError> {
        self.shared.unregister(&mut self.registration);
        let result = self.shared.wait();
        self.shared.join_worker();
        result
    }
}

impl Future for ClientShutdown {
    type Output = Result<(), KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poll = this.shared.poll(&mut this.registration, context);
        if poll.is_ready() {
            this.shared.join_worker();
        }
        poll
    }
}

impl Drop for ClientShutdown {
    fn drop(&mut self) {
        self.shared.unregister(&mut self.registration);
    }
}

impl core::fmt::Debug for ClientShutdownOwner {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ClientShutdownOwner")
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for ClientShutdown {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ClientShutdown")
            .field("registered", &self.registration.is_some())
            .finish_non_exhaustive()
    }
}
