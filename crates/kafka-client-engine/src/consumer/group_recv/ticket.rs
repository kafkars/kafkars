//! Off-reactor ticket for one coalesced global group receive notification.

use std::sync::Arc;

use crate::completion::NotificationTicket;

use super::GroupConsumerRecvSignal;

pub(crate) struct GroupConsumerRecvTicket {
    signal: Arc<GroupConsumerRecvSignal>,
}

impl GroupConsumerRecvTicket {
    pub(crate) const fn new(signal: Arc<GroupConsumerRecvSignal>) -> Self {
        Self { signal }
    }

    pub(crate) fn restore_notification(self) {
        self.signal.restore_notification();
    }
}

impl NotificationTicket for GroupConsumerRecvTicket {
    fn publish(self) {
        self.signal.publish();
    }
}
