//! Completion-first share close admission, leave progress, and entry removal.

use kafka_client_core::{GroupId, ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase};

use crate::{
    clock::DeadlineCapture,
    completion::{CompletionRegistryError, ReclaimStatus},
};

use super::{
    close_state::{
        ShareConsumerCloseCompletion, ShareConsumerCloseState, ShareConsumerCloseTerminal,
    },
    registry::ShareConsumerRegistry,
    registry_membership::ShareMembershipHostError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareConsumerCloseTurn {
    Idle,
    Progress,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareConsumerCloseAdmissionError {
    UnknownConsumer,
    AlreadyClosing,
    Completion(CompletionRegistryError),
}

impl ShareConsumerRegistry {
    pub(crate) fn has_unclosed_entries(&self) -> bool {
        self.entries.iter().any(|entry| !entry.has_close())
    }

    pub(crate) fn begin_explicit_close(
        &mut self,
        group_id: GroupId,
        capture: DeadlineCapture,
    ) -> Result<ShareConsumerCloseCompletion, ShareConsumerCloseAdmissionError> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.group_id() == group_id)
            .ok_or(ShareConsumerCloseAdmissionError::UnknownConsumer)?;
        if self.entries[index].has_close() {
            return Err(ShareConsumerCloseAdmissionError::AlreadyClosing);
        }
        let (completion_id, observer) = self
            .close_completions
            .reserve()
            .map_err(ShareConsumerCloseAdmissionError::Completion)?;
        self.entries[index]
            .install_close(ShareConsumerCloseState::explicit(capture, completion_id))
            .unwrap_or_else(|()| unreachable!("validated open share close"));
        Ok(observer)
    }

    pub(crate) fn request_control_close(&mut self, capture: DeadlineCapture) {
        self.close_admission();
        for entry in &mut self.entries {
            if !entry.has_close() {
                entry
                    .install_close(ShareConsumerCloseState::control(capture))
                    .unwrap_or_else(|()| unreachable!("selected open share close"));
            }
        }
    }

    pub(super) fn turn_one_close(
        &mut self,
        now: kafka_client_core::Moment,
    ) -> Result<ShareConsumerCloseTurn, ShareMembershipHostError> {
        if let Some(index) = self.entries.iter().position(|entry| {
            entry
                .close()
                .is_some_and(|close| close.terminal().is_some())
        }) {
            return self.publish_and_remove(index);
        }
        let Some(index) = self
            .entries
            .iter()
            .position(super::entry::ShareConsumerEntry::has_close)
        else {
            return Ok(ShareConsumerCloseTurn::Idle);
        };
        if self
            .invalidations
            .blocks_submission(self.entries[index].group_id())
            || self.entries[index].heartbeat_call.is_some()
            || self.entries[index].topic_call.is_some()
        {
            return Ok(ShareConsumerCloseTurn::Blocked);
        }
        let terminal = close_terminal(&mut self.entries[index], now)?;
        if let Some(terminal) = terminal {
            self.entries[index]
                .close_mut()
                .ok_or(ShareMembershipHostError::EffectShape)?
                .retain_share_close_terminal(terminal)
                .map_err(|_terminal| ShareMembershipHostError::EffectShape)?;
            return Ok(ShareConsumerCloseTurn::Progress);
        }
        Ok(ShareConsumerCloseTurn::Blocked)
    }

    pub(crate) fn reclaim_one_close_completion(&mut self) -> Result<bool, CompletionRegistryError> {
        let Some(id) = self.close_completions.next_reclaim()? else {
            return Ok(false);
        };
        match self.close_completions.finish_reclaim(id) {
            Ok(ReclaimStatus::Reclaimed) | Err(CompletionRegistryError::GenerationExhausted) => {
                Ok(true)
            }
            Ok(ReclaimStatus::Retry) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn publish_and_remove(
        &mut self,
        index: usize,
    ) -> Result<ShareConsumerCloseTurn, ShareMembershipHostError> {
        let close = self.entries[index]
            .close()
            .ok_or(ShareMembershipHostError::EffectShape)?;
        let terminal = close
            .terminal()
            .ok_or(ShareMembershipHostError::EffectShape)?;
        if let Some(completion_id) = close.completion_id() {
            match self.close_completions.publish(completion_id, terminal) {
                Ok(()) => {}
                Err((CompletionRegistryError::NotificationBackpressure, _terminal)) => {
                    return Ok(ShareConsumerCloseTurn::Blocked);
                }
                Err((_error, _terminal)) => {
                    return Err(ShareMembershipHostError::EffectShape);
                }
            }
        }
        let entry = self.entries.swap_remove(index);
        self.retained_name_bytes = self
            .retained_name_bytes
            .checked_sub(entry.retained_name_bytes())
            .ok_or(ShareMembershipHostError::EffectShape)?;
        drop(entry);
        Ok(ShareConsumerCloseTurn::Progress)
    }
}

fn close_terminal(
    entry: &mut super::entry::ShareConsumerEntry,
    now: kafka_client_core::Moment,
) -> Result<Option<ShareConsumerCloseTerminal>, ShareMembershipHostError> {
    let close_capture = entry
        .close()
        .ok_or(ShareMembershipHostError::EffectShape)?
        .capture();
    if let Some(failure) = entry.fault {
        if let Some(membership) = &mut entry.membership
            && membership.machine().phase() != ShareGroupHeartbeatPhase::Closed
        {
            membership.close_locally()?;
        }
        return Ok(Some(ShareConsumerCloseTerminal::Failed(failure)));
    }
    let Some(membership) = &mut entry.membership else {
        let _start = entry.start.take();
        return Ok(Some(ShareConsumerCloseTerminal::Succeeded));
    };
    let phase = membership.machine().phase();
    match phase {
        ShareGroupHeartbeatPhase::Closed => Ok(Some(ShareConsumerCloseTerminal::Succeeded)),
        ShareGroupHeartbeatPhase::Fatal => {
            let failure = membership.machine().fatal().map_or(
                ShareGroupHeartbeatFailure::Execution,
                kafka_client_core::ShareGroupHeartbeatFatal::failure,
            );
            membership.close_locally()?;
            Ok(Some(ShareConsumerCloseTerminal::Failed(failure)))
        }
        ShareGroupHeartbeatPhase::Dormant | ShareGroupHeartbeatPhase::Joining => {
            membership.close_locally()?;
            Ok(Some(ShareConsumerCloseTerminal::Succeeded))
        }
        ShareGroupHeartbeatPhase::Stable | ShareGroupHeartbeatPhase::AwaitingAssignment => {
            if close_capture.deadline().is_elapsed_at(now) {
                membership.close_locally()?;
                return Ok(Some(ShareConsumerCloseTerminal::Failed(
                    ShareGroupHeartbeatFailure::DeadlineElapsed,
                )));
            }
            membership.begin_leave(close_capture)?;
            Ok(None)
        }
        ShareGroupHeartbeatPhase::Heartbeating => {
            membership.close_locally()?;
            Ok(Some(ShareConsumerCloseTerminal::Failed(
                ShareGroupHeartbeatFailure::Execution,
            )))
        }
        ShareGroupHeartbeatPhase::Leaving => {
            let prepared = membership
                .prepared()
                .ok_or(ShareMembershipHostError::EffectShape)?;
            if prepared.deadline.core().is_elapsed_at(now) {
                membership.expire_prepared_deadline(now)?;
                return Ok(Some(ShareConsumerCloseTerminal::Failed(
                    ShareGroupHeartbeatFailure::DeadlineElapsed,
                )));
            }
            Ok(None)
        }
    }
}
