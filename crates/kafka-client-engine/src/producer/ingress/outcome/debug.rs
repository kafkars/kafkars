//! Non-exhaustive diagnostics for committed producer admission.

use super::ProducerPortAccepted;

impl std::fmt::Debug for ProducerPortAccepted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerPortAccepted")
            .field("observer", &self.observer)
            .field("operation_id", &self.operation_id)
            .field("waiting", &self.waiting.is_some())
            .field("fault", &self.fault)
            .finish()
    }
}
