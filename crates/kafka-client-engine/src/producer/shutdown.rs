//! Producer admission closure and terminal notifier handoff.

use crate::completion::{CompletionRegistryError, NotifierJoin};

use super::ProducerHost;

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
}
