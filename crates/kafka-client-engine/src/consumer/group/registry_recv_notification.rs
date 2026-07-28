//! Exact and global receive-notification publication for the group shard.

use std::sync::Arc;

use crate::consumer::group_recv::{GroupConsumerRecvTicket, GroupConsumerRecvWait};

use super::registry_shard::GroupConsumerShardState;

impl GroupConsumerShardState {
    pub(crate) fn request_group_recv_notification(&self, wake: GroupConsumerRecvWait) {
        if !self.group_recv_signal().prepare_notification(wake) {
            return;
        }
        self.publish_group_recv_notification();
    }

    fn publish_group_recv_notification(&self) {
        let ticket = GroupConsumerRecvTicket::new(Arc::clone(self.group_recv_signal()));
        if let Err(ticket) = self.group_recv_publisher().try_publish(ticket) {
            // `notification_queued` admits at most one ticket, so an open
            // capacity-one queue cannot saturate. Closure restores the signal
            // bookkeeping so a later change can publish a replacement.
            ticket.restore_notification();
        }
    }
}
