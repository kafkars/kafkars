//! Lifecycle terminal publication through the engine-owned transaction notifier.

use kafka_client_core::{TransactionLifecycleInput, TransactionLifecycleTerminal};

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::host::{TransactionLifecycleHost, TransactionLifecycleHostError};

impl TransactionLifecycleHost {
    pub(super) fn publish_terminal(&mut self) -> Result<bool, TransactionLifecycleHostError> {
        let Some((completion_id, terminal)) = self
            .pending_end
            .as_mut()
            .and_then(|pending| Some((pending.completion_id?, pending.terminal.take()?)))
        else {
            return Ok(false);
        };
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                self.pending_end = None;
                if self.release_after_end {
                    self.release_after_end = false;
                    let transition = self
                        .machine
                        .apply(self.owner_id()?, TransactionLifecycleInput::OwnerLost)?;
                    self.interpret(transition.into_effect(), None)?;
                }
                Ok(true)
            }
            Err((CompletionRegistryError::NotificationBackpressure, terminal)) => {
                self.restore_terminal(terminal)?;
                Ok(false)
            }
            Err((error, terminal)) => {
                self.restore_terminal(terminal)?;
                Err(error.into())
            }
        }
    }

    fn restore_terminal(
        &mut self,
        terminal: TransactionLifecycleTerminal,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.pending_end
            .as_mut()
            .ok_or(TransactionLifecycleHostError::MissingEndOperation)?
            .terminal = Some(terminal);
        Ok(())
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, TransactionLifecycleHostError> {
        let completion_id = if let Some(id) = self.reclaim_pending {
            id
        } else {
            let Some(id) = self.completions.next_reclaim()? else {
                return Ok(false);
            };
            self.reclaim_pending = Some(id);
            id
        };
        match self.completions.finish_reclaim(completion_id)? {
            ReclaimStatus::Retry => Ok(false),
            ReclaimStatus::Reclaimed => {
                self.reclaim_pending = None;
                Ok(true)
            }
        }
    }
}
