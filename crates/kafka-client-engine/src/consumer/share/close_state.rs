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

    pub(super) const fn completion_id(&self) -> Option<CompletionId> {
        self.completion_id
    }

    pub(super) const fn terminal(&self) -> Option<ShareConsumerCloseTerminal> {
        self.terminal
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
