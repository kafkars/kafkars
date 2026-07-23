//! Invalid externally reachable waiting-send methods.

struct ProducerHandle;

impl ProducerHandle {
    pub fn send(&self) {}

    pub fn send_captured(&self) {}
}
