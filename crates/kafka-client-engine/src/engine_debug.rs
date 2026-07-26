//! Stable diagnostic shape for the shared engine handle.

use crate::Engine;

impl std::fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}
