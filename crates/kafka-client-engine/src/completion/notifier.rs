//! Dedicated terminal storage and waker execution away from reactor threads.

use std::{
    fmt,
    sync::Arc,
    thread::{self, JoinHandle, ThreadId},
};

use super::{
    notifier_queue::{NotificationQueue, QueuePushError},
    publish_ticket::PublishTicket,
};

pub(crate) trait NotificationTicket: Send + 'static {
    fn publish(self);
}

impl<T: Send + 'static> NotificationTicket for PublishTicket<T> {
    fn publish(self) {
        PublishTicket::publish(self);
    }
}

pub(crate) struct Notifier<J> {
    queue: Arc<NotificationQueue<J>>,
    handle: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> Notifier<PublishTicket<T>> {
    pub(super) fn start(capacity: usize) -> std::io::Result<Self> {
        Self::start_named(capacity, "kafka-client-completion-notifier")
    }
}

impl<J: NotificationTicket> Notifier<J> {
    pub(super) fn start_named(capacity: usize, thread_name: &str) -> std::io::Result<Self> {
        let queue = Arc::new(NotificationQueue::new(capacity));
        let worker_queue = Arc::clone(&queue);
        let handle = thread::Builder::new()
            .name(thread_name.to_owned())
            .spawn(move || run(&worker_queue))?;
        Ok(Self {
            queue,
            handle: Some(handle),
        })
    }

    pub(super) fn stop(mut self) -> NotifierJoin {
        self.queue.close();
        NotifierJoin {
            handle: self.handle.take(),
        }
    }

    pub(super) fn thread_id(&self) -> Option<ThreadId> {
        self.handle.as_ref().map(|handle| handle.thread().id())
    }

    pub(super) fn try_publish(&self, ticket: J) -> Result<(), QueuePushError<J>> {
        self.queue.try_publish(ticket)
    }

    pub(super) fn queue(&self) -> Arc<NotificationQueue<J>> {
        Arc::clone(&self.queue)
    }
}

impl<J> fmt::Debug for Notifier<J> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Notifier")
            .field("running", &self.handle.is_some())
            .finish_non_exhaustive()
    }
}

fn run<J: NotificationTicket>(queue: &NotificationQueue<J>) {
    while let Some(ticket) = queue.next() {
        ticket.publish();
    }
}

/// Join ownership returned by the non-blocking notifier stop boundary.
#[must_use = "join ownership should leave the reactor before it is waited"]
pub(crate) struct NotifierJoin {
    handle: Option<JoinHandle<()>>,
}

impl NotifierJoin {
    /// Waits for notifier termination or returns ownership to its own thread.
    pub(crate) fn join(mut self) -> NotifierJoinOutcome {
        let Some(handle) = self.handle.as_ref() else {
            return NotifierJoinOutcome::Joined(Ok(()));
        };
        if handle.thread().id() == thread::current().id() {
            return NotifierJoinOutcome::SelfThread(self);
        }
        let Some(handle) = self.handle.take() else {
            return NotifierJoinOutcome::Joined(Ok(()));
        };
        NotifierJoinOutcome::Joined(handle.join().map_err(|_panic| NotifierJoinError::Panicked))
    }

    /// Joins from the separately spawned engine-host finalizer.
    ///
    /// Engine startup records the notifier identity before publishing any
    /// handle, and notifier-thread shutdown requests never enter this path.
    pub(crate) fn join_off_notifier(mut self) -> Result<(), NotifierJoinError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle.join().map_err(|_panic| NotifierJoinError::Panicked)
    }

    #[cfg(test)]
    pub(crate) const fn from_handle_for_test(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

impl fmt::Debug for NotifierJoin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotifierJoin")
            .field("joinable", &self.handle.is_some())
            .finish()
    }
}

/// Result that never discards a notifier handle on its own thread.
#[must_use = "a self-thread outcome retains the notifier join owner"]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "ownership-preserving self-join seam is exercised by contract tests"
    )
)]
pub(crate) enum NotifierJoinOutcome {
    Joined(Result<(), NotifierJoinError>),
    SelfThread(NotifierJoin),
}

/// Failure indicating an internal notifier thread panic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotifierJoinError {
    Panicked,
}

impl fmt::Display for NotifierJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("completion notifier panicked")
    }
}

impl std::error::Error for NotifierJoinError {}
