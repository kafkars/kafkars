//! Valid crate-private waiting-send methods.

struct ProducerHandle;

impl ProducerHandle {
    pub(crate) fn send(&self) {}

    pub(crate) fn send_captured(&self) {}
}
