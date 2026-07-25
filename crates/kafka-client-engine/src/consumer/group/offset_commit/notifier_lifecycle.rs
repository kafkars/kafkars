//! Linear notifier identity, fallback transfer, and drop-time cleanup.

use std::thread::ThreadId;

use crate::completion::NotifierJoin;

use super::host::GroupOffsetCommitHost;

impl GroupOffsetCommitHost {
    pub(in crate::consumer::group) fn notifier_thread_id(&self) -> Option<ThreadId> {
        self.completions.notifier_thread_id()
    }

    pub(in crate::consumer::group) fn take_notifier(&mut self) -> Option<NotifierJoin> {
        self.completions.take_notifier()
    }
}

impl Drop for GroupOffsetCommitHost {
    fn drop(&mut self) {
        // Drop is only leak insurance: taking the notifier closes its queue,
        // while explicit startup, shutdown, and recovery paths retain and
        // join their linear owner away from host and notifier execution.
        drop(self.take_notifier());
    }
}
