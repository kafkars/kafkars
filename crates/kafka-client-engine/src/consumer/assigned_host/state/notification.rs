//! Lock-release and receive-notification ordering for the assigned owner.

use std::sync::{Arc, MutexGuard};

use crate::consumer::{
    assigned_host::{
        recv::{AssignedConsumerRecvTicket, AssignedConsumerRecvWait},
        state::AssignedConsumerShardState,
    },
    assigned_owner::AssignedConsumerOwner,
};

impl AssignedConsumerShardState {
    pub(in crate::consumer::assigned_host) fn request_recv_notification(
        &self,
        wake: AssignedConsumerRecvWait,
    ) {
        if !self.recv_signal.prepare_notification(wake) {
            return;
        }
        let ticket = AssignedConsumerRecvTicket::new(Arc::clone(&self.recv_signal));
        if let Err(ticket) = self.recv_publisher.try_publish(ticket) {
            ticket.restore();
        }
    }

    pub(super) fn finish_owner_lock<T>(
        &self,
        guard: MutexGuard<'_, Option<AssignedConsumerOwner>>,
        result: T,
        wake: AssignedConsumerRecvWait,
    ) -> T {
        drop(guard);
        self.request_recv_notification(wake);
        result
    }
}
