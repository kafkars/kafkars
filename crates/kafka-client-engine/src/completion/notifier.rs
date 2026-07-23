//! Dedicated terminal storage and waker execution away from reactor threads.

use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
};

use super::{CompletionId, cell::CompletionCell};

pub(super) struct PublishJob<T> {
    pub(super) id: CompletionId,
    pub(super) cell: Arc<CompletionCell<T>>,
    pub(super) value: T,
}

pub(super) struct Notifier<T> {
    pub(super) sender: SyncSender<PublishJob<T>>,
    handle: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> Notifier<T> {
    pub(super) fn start(capacity: usize) -> std::io::Result<Self> {
        let (sender, receiver) = sync_channel(capacity);
        let handle = thread::Builder::new()
            .name(String::from("kafka-client-completion-notifier"))
            .spawn(move || run(receiver))?;
        Ok(Self {
            sender,
            handle: Some(handle),
        })
    }

    pub(super) fn stop(mut self) -> NotifierJoin {
        drop(self.sender);
        NotifierJoin {
            handle: self.handle.take(),
        }
    }
}

impl<T> fmt::Debug for Notifier<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Notifier")
            .field("running", &self.handle.is_some())
            .finish_non_exhaustive()
    }
}

fn run<T: Send + 'static>(receiver: Receiver<PublishJob<T>>) {
    for job in receiver {
        let outcome = job.cell.store_terminal(job.id, job.value);
        let _ignored = catch_unwind(AssertUnwindSafe(|| drop(outcome.discarded)));
        if outcome.reclaim_after_drop {
            job.cell.queue_reclaim(job.id);
        }
        if let Some(waker) = outcome.waker {
            let _ignored = catch_unwind(AssertUnwindSafe(|| waker.wake()));
        }
    }
}

/// Join ownership returned by the non-blocking notifier stop boundary.
#[must_use = "join ownership should leave the reactor before it is waited"]
pub(crate) struct NotifierJoin {
    handle: Option<JoinHandle<()>>,
}

impl NotifierJoin {
    /// Waits for notifier termination from a management thread.
    pub(crate) fn join(mut self) -> Result<(), NotifierJoinError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle.join().map_err(|_panic| NotifierJoinError)
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

/// Failure indicating an internal notifier thread panic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NotifierJoinError;

impl fmt::Display for NotifierJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("completion notifier panicked")
    }
}

impl std::error::Error for NotifierJoinError {}
