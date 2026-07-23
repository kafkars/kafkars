//! Pending-send admission into the shared bounded completion notifier.

use crate::producer::pending::PendingNotificationJob;

use super::{CompletionRegistry, CompletionRegistryError, notifier_queue::QueuePushError};

impl<T: Send + 'static> CompletionRegistry<T> {
    /// Queues one pending-send state notification on the completion dispatcher.
    ///
    /// The job owns a cell reference and its non-cloneable pending-notification
    /// permit. Its application waker and transition value remain inside that
    /// cell when the global notification FIFO backpressures. Full and stopped
    /// outcomes return that exact typed job; callers must retain it, including
    /// the permit capacity, for later off-reactor retry or recovery.
    pub(crate) fn notify_pending(
        &self,
        job: PendingNotificationJob,
    ) -> Result<(), (CompletionRegistryError, PendingNotificationJob)> {
        let Some(notifier) = &self.notifier else {
            return Err((CompletionRegistryError::NotifierStopped, job));
        };
        match notifier.try_pending(job) {
            Ok(()) => Ok(()),
            Err(QueuePushError::Full(job)) => {
                Err((CompletionRegistryError::NotificationBackpressure, job))
            }
            Err(QueuePushError::Closed(job)) => {
                Err((CompletionRegistryError::NotifierStopped, job))
            }
        }
    }
}
