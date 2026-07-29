//! Call polling, route release, publication, reclamation, and recovery.

mod recovery;

#[cfg(test)]
mod recovery_test;

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::{AdminListTransactionsHost, AdminListTransactionsHostError, response::terminal_input};

impl AdminListTransactionsHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, AdminListTransactionsHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        let terminal = {
            let call = self.operations[index]
                .call
                .as_mut()
                .ok_or(AdminListTransactionsHostError::InvalidHandoff)?;
            call.try_terminal()
        };
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        match terminal {
            Ok(terminal) => {
                drop(self.operations[index].call.take());
                self.operations[index].raw_terminal = Some(terminal);
                self.settle_raw(index)?;
                Ok(true)
            }
            Err(_error) => Err(AdminListTransactionsHostError::CallCompletion),
        }
    }

    fn settle_raw(&mut self, index: usize) -> Result<(), AdminListTransactionsHostError> {
        let (input, retained_bytes) = {
            let operation = self
                .operations
                .get(index)
                .ok_or(AdminListTransactionsHostError::UnknownOperation)?;
            let raw = operation
                .raw_terminal
                .as_ref()
                .ok_or(AdminListTransactionsHostError::MissingTerminal)?;
            if !raw_matches_active_submission(operation, raw) {
                return Err(AdminListTransactionsHostError::SubmissionMismatch);
            }
            terminal_input(raw)
        };
        self.operations[index].remaining_result_bytes = self.operations[index]
            .remaining_result_bytes
            .checked_sub(retained_bytes)
            .ok_or(AdminListTransactionsHostError::ByteAccounting)?;
        let transition = self.operations[index].machine.apply(input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(AdminListTransactionsHostError::MissingTerminal)?;
        raw.discard();
        let effect = transition
            .into_effect()
            .ok_or(AdminListTransactionsHostError::MissingTerminal)?;
        self.install_effect(index, effect)
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), AdminListTransactionsHostError> {
        if self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AdminListTransactionsHostError::InvalidHandoff);
        }
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(AdminListTransactionsHostError::MissingTerminal)?;
        let completion_id = self.operations[index].completion_id;
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                let operation = self.operations.remove(index);
                self.published_bytes
                    .push((operation.completion_id, operation.retained_bytes));
                Ok(())
            }
            Err((error, terminal)) => {
                self.operations[index].terminal = Some(terminal);
                Err(AdminListTransactionsHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, AdminListTransactionsHostError> {
        let completion_id = if let Some(id) = self.reclaim_pending {
            id
        } else {
            let Some(id) = self.completions.next_reclaim()? else {
                return Ok(false);
            };
            self.reclaim_pending = Some(id);
            id
        };
        match self.completions.finish_reclaim(completion_id) {
            Ok(ReclaimStatus::Retry) => Ok(false),
            Ok(ReclaimStatus::Reclaimed) | Err(CompletionRegistryError::GenerationExhausted) => {
                self.release_published_bytes(completion_id)?;
                self.reclaim_pending = None;
                Ok(true)
            }
            Err(error) => Err(AdminListTransactionsHostError::Completion(error)),
        }
    }

    fn release_published_bytes(
        &mut self,
        completion_id: crate::completion::CompletionId,
    ) -> Result<(), AdminListTransactionsHostError> {
        let index = self
            .published_bytes
            .iter()
            .position(|(id, _)| *id == completion_id)
            .ok_or(AdminListTransactionsHostError::ByteAccounting)?;
        let (_, bytes) = self.published_bytes.swap_remove(index);
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(bytes)
            .ok_or(AdminListTransactionsHostError::ByteAccounting)?;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::admin::list_transactions) fn retain_raw_terminal_for_test(
        &mut self,
        raw: crate::driver::ListTransactionsRawTerminal,
    ) {
        self.operations[0].raw_terminal = Some(raw);
    }

    #[cfg(test)]
    pub(in crate::admin::list_transactions) fn settle_raw_for_test(
        &mut self,
    ) -> Result<(), AdminListTransactionsHostError> {
        self.settle_raw(0)
    }

    #[cfg(test)]
    pub(in crate::admin::list_transactions) fn recovered_matches_discovery_for_test(
        &self,
        retained_limit: usize,
    ) -> bool {
        self.operations[0]
            .recovered_call
            .as_ref()
            .is_some_and(|recovered| recovered.matches_discovery(retained_limit))
    }

    #[cfg(test)]
    pub(in crate::admin::list_transactions) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AdminListTransactionsHostError> {
        self.publish_terminal(0)
    }
}

fn raw_matches_active_submission(
    operation: &super::AdminListTransactionsOperation,
    raw: &crate::driver::ListTransactionsRawTerminal,
) -> bool {
    let Some(submission) = operation.active_submission.as_ref() else {
        return false;
    };
    match submission {
        super::AdminListTransactionsSubmissionKind::Discovery { retained_limit } => {
            raw.matches_discovery(*retained_limit)
        }
        super::AdminListTransactionsSubmissionKind::Broker {
            broker_id,
            plan,
            retained_limit,
        } => raw.matches_broker(*broker_id, plan, *retained_limit),
    }
}
