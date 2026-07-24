//! One bounded notifier worker for concrete assigned-consumer observations.

use std::thread::ThreadId;

use crate::completion::{
    CompletionRegistryError, NotificationTicket, NotifierJoin, PublishTicket, SharedNotifier,
    SharedPublishPort,
};

use super::close_observer::AssignedConsumerCloseTerminal;

const ASSIGNED_CONSUMER_NOTIFIER_THREAD: &str =
    "kafka-client-assigned-consumer-completion-notifier";
const ASSIGNED_CONSUMER_NOTIFICATION_CAPACITY: usize = 1;

/// Closed ticket set accepted by the assigned-consumer notifier.
pub(crate) enum AssignedConsumerPublishTicket {
    Close(PublishTicket<AssignedConsumerCloseTerminal>),
}

impl NotificationTicket for AssignedConsumerPublishTicket {
    fn publish(self) {
        match self {
            Self::Close(ticket) => ticket.publish(),
        }
    }
}

pub(crate) type AssignedConsumerClosePublisher =
    SharedPublishPort<AssignedConsumerCloseTerminal, AssignedConsumerPublishTicket>;

/// Unique lifecycle owner for the assigned-consumer notifier.
pub(crate) struct AssignedConsumerCompletionNotifier {
    worker: Option<SharedNotifier<AssignedConsumerPublishTicket>>,
}

impl AssignedConsumerCompletionNotifier {
    pub(crate) fn start() -> std::io::Result<(Self, AssignedConsumerClosePublisher)> {
        let worker = SharedNotifier::start(
            ASSIGNED_CONSUMER_NOTIFICATION_CAPACITY,
            ASSIGNED_CONSUMER_NOTIFIER_THREAD,
        )?;
        let close = worker.publish_port(AssignedConsumerPublishTicket::Close);
        Ok((
            Self {
                worker: Some(worker),
            },
            close,
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
