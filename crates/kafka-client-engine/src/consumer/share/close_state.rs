//! Exact close deadline, completion identity, and retryable terminal ownership.

use kafka_client_core::ShareGroupHeartbeatFailure;

use crate::{
    clock::DeadlineCapture,
    completion::{CompletionId, CompletionObserver},
};

/// Stable terminal for one accepted share-consumer close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerCloseTerminal {
    Succeeded,
    Failed(ShareGroupHeartbeatFailure),
}

pub(super) struct ShareConsumerCloseState {
    capture: DeadlineCapture,
    completion_id: Option<CompletionId>,
    terminal: Option<ShareConsumerCloseTerminal>,
}

impl ShareConsumerCloseState {
    pub(super) const fn control(capture: DeadlineCapture) -> Self {
        Self {
            capture,
            completion_id: None,
            terminal: None,
        }
    }

    pub(super) const fn explicit(capture: DeadlineCapture, completion_id: CompletionId) -> Self {
        Self {
            capture,
            completion_id: Some(completion_id),
            terminal: None,
        }
    }

    pub(super) const fn capture(&self) -> DeadlineCapture {
        self.capture
    }

    pub(super) const fn deadline(&self) -> kafka_client_core::Deadline {
        self.capture.deadline()
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        (self.terminal.is_none() || self.completion_id.is_some()).then_some(self.deadline())
    }

    pub(super) const fn completion_id(&self) -> Option<CompletionId> {
        self.completion_id
    }

    pub(super) const fn terminal(&self) -> Option<ShareConsumerCloseTerminal> {
        self.terminal
    }

    pub(super) fn mark_share_close_published(&mut self, id: CompletionId) -> bool {
        if self.completion_id != Some(id) || self.terminal.is_none() {
            return false;
        }
        self.completion_id = None;
        true
    }

    pub(super) fn retain_share_close_terminal(
        &mut self,
        terminal: ShareConsumerCloseTerminal,
    ) -> Result<(), ShareConsumerCloseTerminal> {
        if self.terminal.is_some() {
            return Err(terminal);
        }
        self.terminal = Some(terminal);
        Ok(())
    }
}

impl super::entry::ShareConsumerEntry {
    pub(super) fn expire_share_close(&mut self, now: kafka_client_core::Moment) -> bool {
        let failure = self
            .share_membership_failure()
            .unwrap_or(ShareGroupHeartbeatFailure::DeadlineElapsed);
        let Some(close) = self.close_mut() else {
            return false;
        };
        if close.completion_id.is_some()
            && close.capture.deadline().is_elapsed_at(now)
            && matches!(
                close.terminal,
                None | Some(ShareConsumerCloseTerminal::Succeeded)
            )
        {
            close.terminal = Some(ShareConsumerCloseTerminal::Failed(failure));
            return true;
        }
        false
    }

    pub(super) fn share_close_terminal_is_actionable(&self, invalidation_pending: bool) -> bool {
        self.close().is_some_and(|close| {
            close.terminal.is_some()
                && (close.completion_id.is_some()
                    || (!invalidation_pending
                        && !self.share_close_has_retained_calls()
                        && !self.fetch().blocks_close()))
        })
    }

    pub(super) fn share_close_needs_terminal(&self) -> bool {
        self.close().is_some_and(|close| close.terminal.is_none())
    }

    pub(super) const fn close(&self) -> Option<&ShareConsumerCloseState> {
        self.close.as_ref()
    }

    pub(super) fn close_mut(&mut self) -> Option<&mut ShareConsumerCloseState> {
        self.close.as_mut()
    }

    pub(super) const fn has_close(&self) -> bool {
        self.close.is_some()
    }

    pub(super) fn install_close(&mut self, close: ShareConsumerCloseState) -> Result<(), ()> {
        if self.close.is_some() {
            return Err(());
        }
        self.close = Some(close);
        Ok(())
    }
}

pub(crate) type ShareConsumerCloseCompletion = CompletionObserver<ShareConsumerCloseTerminal>;
