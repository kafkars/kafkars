//! Shared public owner of one reactor-native execution host.

use std::sync::{Arc, Mutex};

use crate::{
    EngineConfig, ProducerHandle,
    engine_host::{
        EngineHostJoin, EngineShutdownError, EngineStartError, StartedEngineHost,
        start as start_host,
    },
};

/// Cheaply cloneable owner of one embedded driver and producer host.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    admission: crate::producer::ingress::ProducerAdmissionPort,
    clock: Arc<crate::clock::MonotonicClock>,
    host: Mutex<Option<EngineHostJoin>>,
}

impl Engine {
    /// Validates every local bound and starts one native host thread.
    pub fn start(config: EngineConfig) -> Result<Self, EngineStartError> {
        let validated = config.validate().map_err(EngineStartError::configuration)?;
        let StartedEngineHost {
            admission,
            clock,
            join,
        } = start_host(&config, validated)?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                admission,
                clock,
                host: Mutex::new(Some(join)),
            }),
        })
    }

    /// Returns a runtime-neutral producer handle retaining this host.
    pub fn producer(&self) -> ProducerHandle {
        let lifetime: Arc<dyn Send + Sync> = self.inner.clone();
        ProducerHandle::from_port(
            self.inner.admission.clone(),
            Arc::clone(&self.inner.clock),
            lifetime,
        )
    }

    /// Returns immutable engine configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    /// Closes admission, drains accepted work, and joins native resources.
    pub fn shutdown(&self) -> Result<(), EngineShutdownError> {
        self.inner.shutdown()
    }

    #[cfg(test)]
    pub(crate) fn host_snapshot(&self) -> crate::engine_host::EngineHostSnapshot {
        match self.inner.host.lock() {
            Ok(host) => host
                .as_ref()
                .map_or(crate::engine_host::EngineHostSnapshot::default(), |host| {
                    host.snapshot()
                }),
            Err(_) => crate::engine_host::EngineHostSnapshot::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn force_host_failure(&self) {
        if let Ok(host) = self.inner.host.lock()
            && let Some(host) = host.as_ref()
        {
            host.force_failure();
        }
    }
}

impl EngineInner {
    fn shutdown(&self) -> Result<(), EngineShutdownError> {
        let mut host = self
            .host
            .lock()
            .map_err(|_poisoned| EngineShutdownError::lock_poisoned())?;
        let Some(mut host) = host.take() else {
            return Ok(());
        };
        host.shutdown_and_join()
            .map_err(|error| EngineShutdownError::host(&error))
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        let host = match self.host.get_mut() {
            Ok(host) => host,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(host) = host.take() {
            host.shutdown_detached();
        }
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}
