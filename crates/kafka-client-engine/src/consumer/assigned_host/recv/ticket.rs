//! Off-reactor notification ticket for one exact receive generation.

use std::sync::Arc;

use super::AssignedConsumerRecvSignal;

/// Linear notifier ownership of one receive wake.
pub(crate) struct AssignedConsumerRecvTicket {
    signal: Arc<AssignedConsumerRecvSignal>,
}

impl AssignedConsumerRecvTicket {
    pub(crate) const fn new(signal: Arc<AssignedConsumerRecvSignal>) -> Self {
        Self { signal }
    }

    pub(crate) fn publish(self) {
        self.signal.publish();
    }

    pub(crate) fn restore(self) {
        self.signal.restore_notification();
    }
}
