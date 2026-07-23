//! Producer admission closure and terminal notifier handoff.

use std::thread::ThreadId;

use crate::completion::{CompletionRegistryError, NotifierJoin};

use super::ProducerHost;

/// Cleanup ownership retained even when terminal recovery remains damaged.
pub(crate) struct ProducerNotifierRecovery {
    pub(crate) notifier: Option<NotifierJoin>,
    pub(crate) error: Option<CompletionRegistryError>,
}

impl ProducerHost {
    /// Permanently closes core admission before terminal host drain.
    pub(crate) const fn close_admission(&mut self) {
        self.core.close_admission();
    }

    /// Returns accepted operations that still owe terminal publication.
    pub(crate) fn unsettled_completions(&self) -> usize {
        self.completions.unsettled_len()
    }

    /// Stops the notifier without waiting after every terminal publishes.
    pub(crate) fn stop_notifier(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.completions.stop_notifier()
    }

    /// Transfers terminal cleanup ownership after catastrophic recovery.
    pub(crate) fn recover_notifier(&mut self) -> ProducerNotifierRecovery {
        let error = (self.unsettled_completions() != 0)
            .then_some(CompletionRegistryError::UnsettledCompletion)
            .or_else(|| {
                self.completions
                    .notifier_thread_id()
                    .is_none()
                    .then_some(CompletionRegistryError::NotifierStopped)
            });
        ProducerNotifierRecovery {
            notifier: self.completions.take_notifier(),
            error,
        }
    }

    /// Returns the thread that exclusively executes completion wakeups.
    pub(crate) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.completions.notifier_thread_id()
    }
}
