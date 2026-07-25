//! Exact terminal publication, observation reclaim, and byte release.

use crate::completion::{CompletionRegistryError, ReclaimStatus};

use super::host::{GroupOffsetCommitHost, GroupOffsetCommitHostError};

impl GroupOffsetCommitHost {
    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let terminal = self.operations[index]
            .replace_terminal(None)
            .ok_or(GroupOffsetCommitHostError::MissingTerminal)?;
        let completion_id = self.operations[index].completion_id;
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                let operation = self.operations.remove(index);
                self.published_bytes
                    .push((completion_id, operation.byte_charge));
                Ok(())
            }
            Err((error, terminal)) => {
                self.operations[index].replace_terminal(Some(terminal));
                Err(GroupOffsetCommitHostError::Completion(error))
            }
        }
    }

    pub(super) fn reclaim_one(&mut self) -> Result<bool, GroupOffsetCommitHostError> {
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
                let index = self
                    .published_bytes
                    .iter()
                    .position(|(id, _bytes)| *id == completion_id)
                    .ok_or(GroupOffsetCommitHostError::ByteAccounting)?;
                let (_id, bytes) = self.published_bytes.swap_remove(index);
                self.retained_bytes = self
                    .retained_bytes
                    .checked_sub(bytes)
                    .ok_or(GroupOffsetCommitHostError::ByteAccounting)?;
                self.reclaim_pending = None;
                Ok(true)
            }
            Err(error) => Err(GroupOffsetCommitHostError::Completion(error)),
        }
    }
}
