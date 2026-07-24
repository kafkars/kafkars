//! One bounded notifier worker for concrete assigned-consumer observations.

use std::thread::ThreadId;

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket,
    SharedNotificationPort, SharedNotifier, SharedPublishPort,
};

use super::{close_observer::AssignedConsumerCloseTerminal, recv::AssignedConsumerRecvTicket};

const ASSIGNED_CONSUMER_NOTIFIER_THREAD: &str =
    "kafka-client-assigned-consumer-completion-notifier";
const ASSIGNED_CONSUMER_CLOSE_CAPACITY: usize = 1;
const ASSIGNED_CONSUMER_RECV_CAPACITY: usize = 1;
const ASSIGNED_CONSUMER_NOTIFICATION_CAPACITY: usize =
    ASSIGNED_CONSUMER_CLOSE_CAPACITY + ASSIGNED_CONSUMER_RECV_CAPACITY;

/// Closed ticket set accepted by the assigned-consumer notifier.
pub(crate) enum AssignedConsumerPublishTicket {
    Close(PublishTicket<AssignedConsumerCloseTerminal>),
    Recv(AssignedConsumerRecvTicket),
}

impl NotificationTicket for AssignedConsumerPublishTicket {
    fn publish(self) {
        match self {
            Self::Close(ticket) => ticket.publish(),
            Self::Recv(ticket) => ticket.publish(),
        }
    }
}

pub(crate) type AssignedConsumerClosePublisher =
    SharedPublishPort<AssignedConsumerCloseTerminal, AssignedConsumerPublishTicket>;
pub(crate) type AssignedConsumerRecvPublisher =
    SharedNotificationPort<AssignedConsumerRecvTicket, AssignedConsumerPublishTicket>;

/// Exact typed ports issued by the sole assigned-consumer notifier.
pub(crate) struct AssignedConsumerCompletionPorts {
    pub(crate) close: AssignedConsumerClosePublisher,
    pub(crate) recv: AssignedConsumerRecvPublisher,
}

/// Unique lifecycle owner for the assigned-consumer notifier.
pub(crate) struct AssignedConsumerCompletionNotifier {
    worker: Option<SharedNotifier<AssignedConsumerPublishTicket>>,
}

impl AssignedConsumerCompletionNotifier {
    pub(crate) fn start() -> std::io::Result<(Self, AssignedConsumerCompletionPorts)> {
        let worker = SharedNotifier::start(
            ASSIGNED_CONSUMER_NOTIFICATION_CAPACITY,
            ASSIGNED_CONSUMER_NOTIFIER_THREAD,
        )?;
        let close = worker.publish_port(AssignedConsumerPublishTicket::Close);
        let recv = worker.notification_port(AssignedConsumerPublishTicket::Recv);
        Ok((
            Self {
                worker: Some(worker),
            },
            AssignedConsumerCompletionPorts { close, recv },
        ))
    }

    pub(crate) fn stop(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.take_join()
            .ok_or(CompletionRegistryError::NotifierStopped)
    }

    pub(crate) fn take_join(&mut self) -> Option<NotifierJoin> {
        self.worker.take().map(SharedNotifier::stop)
    }

    pub(crate) fn thread_id(&self) -> Option<ThreadId> {
        self.worker.as_ref().and_then(SharedNotifier::thread_id)
    }
}
