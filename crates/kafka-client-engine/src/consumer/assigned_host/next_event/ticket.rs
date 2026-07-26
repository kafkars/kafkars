//! Off-reactor notification ticket for the current event registration.

use std::sync::Arc;

use super::AssignedConsumerEventSignal;

/// Linear notifier ownership of one assigned-consumer event wake.
pub(crate) struct AssignedConsumerEventTicket {
    signal: Arc<AssignedConsumerEventSignal>,
}

impl AssignedConsumerEventTicket {
    pub(crate) const fn new(signal: Arc<AssignedConsumerEventSignal>) -> Self {
        Self { signal }
    }

    pub(crate) fn publish(self) {
        self.signal.publish();
    }

    pub(crate) fn restore(self) {
        self.signal.restore_notification();
    }
}
