//! Dedicated producer-notifier construction and linear shutdown ownership.

use std::thread::ThreadId;

use super::CompletionRegistry;
use crate::completion::{CompletionRegistryError, NotifierJoin, notifier::Notifier};

impl<T: Send + 'static> CompletionRegistry<T> {
    pub(crate) fn start(capacity: usize) -> std::io::Result<Self> {
        Ok(Self::with_publisher(capacity, Notifier::start(capacity)?))
    }

    /// Stops notification without waiting; joining belongs off-reactor.
    pub(crate) fn stop_notifier(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        if self.unsettled_len() != 0 {
            return Err(CompletionRegistryError::UnsettledCompletion);
        }
        let Some(notifier) = self.publisher.take() else {
            return Err(CompletionRegistryError::NotifierStopped);
        };
        Ok(notifier.stop())
    }

    /// Transfers notifier cleanup ownership even after a recovery failure.
    pub(crate) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.publisher.take().map(Notifier::stop)
    }

    /// Returns the dedicated notifier identity for reentrant-shutdown fencing.
    pub(crate) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.publisher.as_ref().and_then(Notifier::thread_id)
    }
}
