//! Share close notifier identity and linear shutdown handoff.

use std::thread::ThreadId;

use crate::completion::{CompletionRegistryError, NotifierJoin};

use super::registry::ShareConsumerRegistry;

impl ShareConsumerRegistry {
    pub(crate) fn close_notifier_thread_id(&self) -> Option<ThreadId> {
        self.close_completions.notifier_thread_id()
    }

    pub(crate) fn stop_close_notifier(&mut self) -> Result<NotifierJoin, CompletionRegistryError> {
        self.close_completions.stop_notifier()
    }

    pub(crate) fn take_close_notifier(&mut self) -> Option<NotifierJoin> {
        self.close_completions.take_notifier()
    }
}

impl Drop for ShareConsumerRegistry {
    fn drop(&mut self) {
        drop(self.take_close_notifier());
    }
}
