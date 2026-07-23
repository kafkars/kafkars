//! Dedicated terminal storage and waker execution away from reactor threads.

use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    thread::{self, JoinHandle, ThreadId},
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

    pub(super) fn thread_id(&self) -> Option<ThreadId> {
        self.handle.as_ref().map(|handle| handle.thread().id())
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
    /// Waits for notifier termination or returns ownership to its own thread.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "ownership-preserving self-join seam is exercised by contract tests"
        )
    )]
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
