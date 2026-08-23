//! Exact global receive-notification publication for the share shard.

use std::sync::Arc;

use crate::consumer::share_recv::{ShareConsumerRecvTicket, ShareConsumerRecvWait};

use super::shard::ShareConsumerShardState;

impl ShareConsumerShardState {
    pub(crate) fn request_share_recv_notification(&self, wake: ShareConsumerRecvWait) {
        if !self.share_recv_signal().prepare_notification(wake) {
            return;
        }
        let ticket = ShareConsumerRecvTicket::new(Arc::clone(self.share_recv_signal()));
        if let Err(ticket) = self.share_recv_publisher().try_publish(ticket) {
            ticket.restore_notification();
        }
    }
}
