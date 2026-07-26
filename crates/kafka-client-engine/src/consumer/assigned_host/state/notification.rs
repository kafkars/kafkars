//! Lock-release and receive-notification ordering for the assigned owner.

use std::sync::{Arc, MutexGuard};

use crate::consumer::{
    assigned_host::{
        next_event::{AssignedConsumerEventTicket, AssignedConsumerEventWait},
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

    pub(in crate::consumer::assigned_host) fn request_event_notification(
        &self,
        wake: AssignedConsumerEventWait,
    ) {
        if !self.event_signal.prepare_notification(wake) {
            return;
        }
        let ticket = AssignedConsumerEventTicket::new(Arc::clone(&self.event_signal));
        if let Err(ticket) = self.event_publisher.try_publish(ticket) {
            ticket.restore();
        }
    }

    pub(in crate::consumer::assigned_host) fn request_observation_unlock_notifications(&self) {
        self.request_recv_notification(AssignedConsumerRecvWait::Unlock);
        self.request_event_notification(AssignedConsumerEventWait::Unlock);
    }

    pub(in crate::consumer::assigned_host) fn request_observation_change_notifications(&self) {
        self.request_recv_notification(AssignedConsumerRecvWait::Change);
        self.request_event_notification(AssignedConsumerEventWait::Change);
    }

    pub(super) fn finish_owner_lock<T>(
        &self,
        guard: MutexGuard<'_, Option<AssignedConsumerOwner>>,
        result: T,
        wake: AssignedConsumerRecvWait,
    ) -> T {
        drop(guard);
        self.request_recv_notification(wake);
        let event_wake = match wake {
            AssignedConsumerRecvWait::Change => AssignedConsumerEventWait::Change,
            AssignedConsumerRecvWait::Unlock => AssignedConsumerEventWait::Unlock,
        };
        self.request_event_notification(event_wake);
        result
    }
}
