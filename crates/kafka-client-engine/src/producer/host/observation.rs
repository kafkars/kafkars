//! Read-only observations over producer host state.

use kafka_client_core::ProducerEffect;

use super::ProducerHost;

impl ProducerHost {
    pub(crate) fn pending_effects(&self) -> &[ProducerEffect] {
        &self.pending_effects
    }

    /// Reports the deterministic core's producer admission decision.
    pub(crate) const fn admission_is_open(&self) -> bool {
        self.core.admission_is_open()
    }
}
