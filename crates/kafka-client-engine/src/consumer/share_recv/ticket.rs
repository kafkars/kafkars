//! Off-reactor ticket for one coalesced global share receive notification.

use std::sync::Arc;

use crate::completion::NotificationTicket;

use super::ShareConsumerRecvSignal;

pub(crate) struct ShareConsumerRecvTicket {
    signal: Arc<ShareConsumerRecvSignal>,
}

impl ShareConsumerRecvTicket {
    pub(crate) const fn new(signal: Arc<ShareConsumerRecvSignal>) -> Self {
        Self { signal }
    }

    pub(crate) fn restore_notification(self) {
        self.signal.restore_notification();
    }
}

impl NotificationTicket for ShareConsumerRecvTicket {
    fn publish(self) {
        self.signal.publish();
    }
}
