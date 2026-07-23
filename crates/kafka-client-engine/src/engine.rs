//! Shared public owner of one reactor-native execution host.

use std::sync::Arc;

use crate::{
    EngineConfig, ProducerHandle,
    engine_host::{
        EngineHostControl, EngineLifecycle, EngineShutdownError, EngineStartError,
        StartedEngineHost, start as start_host,
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
    control: Arc<EngineHostControl>,
    lifecycle: Arc<EngineLifecycle>,
}

impl Engine {
    /// Validates every local bound and starts one native host thread.
    pub fn start(config: EngineConfig) -> Result<Self, EngineStartError> {
        let validated = config.validate().map_err(EngineStartError::configuration)?;
        let StartedEngineHost {
            admission,
            clock,
            control,
            lifecycle,
        } = start_host(&config, validated)?;
        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                admission,
                clock,
                control,
                lifecycle,
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

    /// Waits for the host's retained report after native resource cleanup.
    pub fn shutdown(&self) -> Result<(), EngineShutdownError> {
        self.inner.shutdown()
    }

    #[cfg(test)]
    pub(crate) fn host_snapshot(&self) -> crate::engine_host::EngineHostSnapshot {
        self.inner.control.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn force_host_failure(&self) {
        self.inner.control.request_failure();
    }

    #[cfg(test)]
    pub(crate) fn host_is_closed(&self) -> bool {
        self.inner.lifecycle.is_closed()
    }

    #[cfg(test)]
    pub(crate) fn host_is_closing(&self) -> bool {
        self.inner.lifecycle.is_closing()
    }

    #[cfg(test)]
    pub(crate) fn host_probe(&self) -> EngineHostProbe {
        EngineHostProbe {
            lifecycle: Arc::clone(&self.inner.lifecycle),
        }
    }
}

impl EngineInner {
    fn shutdown(&self) -> Result<(), EngineShutdownError> {
        let _close_result = self.admission.close_admission();
        self.lifecycle.request_and_wait(&self.control)
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        let _close_result = self.admission.close_admission();
        self.lifecycle.request(&self.control);
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

#[cfg(test)]
pub(crate) struct EngineHostProbe {
    lifecycle: Arc<EngineLifecycle>,
}

#[cfg(test)]
impl EngineHostProbe {
    pub(crate) fn wait_closed(&self, timeout: std::time::Duration) -> bool {
        self.lifecycle.wait_closed(timeout)
    }
}
