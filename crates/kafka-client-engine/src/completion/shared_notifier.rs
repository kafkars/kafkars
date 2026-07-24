//! Allocation-free typed ports into one bounded heterogeneous notifier worker.

use std::{marker::PhantomData, sync::Arc, thread::ThreadId};

use super::{
    notifier::{NotificationTicket, Notifier},
    notifier_queue::{NotificationQueue, QueuePushError},
    publish_ticket::PublishTicket,
    registry::CompletionPublisher,
};

/// One typed registry port into a shared closed ticket domain.
pub(crate) struct SharedPublishPort<T, J> {
    queue: Arc<NotificationQueue<J>>,
    wrap: fn(PublishTicket<T>) -> J,
    _terminal: PhantomData<fn(T)>,
}

impl<T, J> SharedPublishPort<T, J> {
    pub(super) fn new(queue: Arc<NotificationQueue<J>>, wrap: fn(PublishTicket<T>) -> J) -> Self {
        Self {
            queue,
            wrap,
            _terminal: PhantomData,
        }
    }

    pub(super) fn try_publish(
        &self,
        ticket: PublishTicket<T>,
    ) -> Result<(), QueuePushError<PublishTicket<T>>> {
        self.queue.try_publish_with(ticket, self.wrap)
    }
}

impl<T, J> CompletionPublisher<T> for SharedPublishPort<T, J>
where
    T: Send + 'static,
    J: NotificationTicket,
{
    fn try_publish(
        &self,
        ticket: PublishTicket<T>,
    ) -> Result<(), QueuePushError<PublishTicket<T>>> {
        SharedPublishPort::try_publish(self, ticket)
    }
}

/// Unique shared worker owner and typed-port factory.
pub(crate) struct SharedNotifier<J> {
    notifier: Notifier<J>,
}

impl<J: NotificationTicket> SharedNotifier<J> {
    pub(crate) fn start(capacity: usize, thread_name: &str) -> std::io::Result<Self> {
        Notifier::start_named(capacity, thread_name).map(|notifier| Self { notifier })
    }

    pub(crate) fn publish_port<T>(
        &self,
        wrap: fn(PublishTicket<T>) -> J,
    ) -> SharedPublishPort<T, J> {
        SharedPublishPort::new(self.notifier.queue(), wrap)
    }

    pub(crate) fn stop(self) -> super::NotifierJoin {
        self.notifier.stop()
    }

    pub(crate) fn thread_id(&self) -> Option<ThreadId> {
        self.notifier.thread_id()
    }
}
