//! Completion-first acknowledgement admission, publication, and reclamation.

use kafka_client_core::GroupId;

use crate::{
    clock::DeadlineCapture,
    completion::{CompletionObserver, CompletionRegistryError, ReclaimStatus},
    consumer::{
        ShareAcknowledgeOutcome, share_acknowledge::ShareAcknowledgementCompletionOwner,
        share_batch::ShareAcknowledgementAdmissionParts,
    },
};

use super::{
    fetch_session_set::ShareSessionAcknowledgementAdmissionFailureKind,
    registry::ShareConsumerRegistry, registry_membership::ShareMembershipHostError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareAcknowledgementAdmissionFailureKind {
    UnknownConsumer,
    Completion(CompletionRegistryError),
    Session(ShareSessionAcknowledgementAdmissionFailureKind),
    Rollback(CompletionRegistryError),
}

#[must_use = "rejected acknowledgement admission retains exact caller ownership"]
pub(super) struct ShareAcknowledgementAdmissionFailure {
    pub(super) kind: ShareAcknowledgementAdmissionFailureKind,
    pub(super) parts: ShareAcknowledgementAdmissionParts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareAcknowledgementCompletionTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(super) fn begin_acknowledgement(
        &mut self,
        group_id: GroupId,
        parts: ShareAcknowledgementAdmissionParts,
        capture: DeadlineCapture,
    ) -> Result<CompletionObserver<ShareAcknowledgeOutcome>, ShareAcknowledgementAdmissionFailure>
    {
        let Some(entry_index) = self
            .entries
            .iter()
            .position(|entry| entry.group_id() == group_id)
        else {
            return Err(ShareAcknowledgementAdmissionFailure {
                kind: ShareAcknowledgementAdmissionFailureKind::UnknownConsumer,
                parts,
            });
        };
        let (completion_id, observer) = match self.acknowledgement_completions.reserve() {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(ShareAcknowledgementAdmissionFailure {
                    kind: ShareAcknowledgementAdmissionFailureKind::Completion(error),
                    parts,
                });
            }
        };
        let ShareAcknowledgementAdmissionParts { inner, recovery } = parts;
        let completion = ShareAcknowledgementCompletionOwner::pending(completion_id, recovery);
        let failure = match self.entries[entry_index].fetch_mut().sessions_mut() {
            Some(sessions) => sessions.prepare_public_acknowledgement(inner, capture, completion),
            None => Err(
                super::fetch_session_set::ShareSessionAcknowledgementAdmissionFailure {
                    kind: ShareSessionAcknowledgementAdmissionFailureKind::UnknownSession,
                    parts: acknowledgement_parts(inner, completion),
                },
            ),
        };
        match failure {
            Ok(()) => Ok(observer),
            Err(failure) => match self
                .acknowledgement_completions
                .rollback_reservation(completion_id)
            {
                Ok(()) => Err(ShareAcknowledgementAdmissionFailure {
                    kind: ShareAcknowledgementAdmissionFailureKind::Session(failure.kind),
                    parts: failure.parts,
                }),
                Err(error) => Err(ShareAcknowledgementAdmissionFailure {
                    kind: ShareAcknowledgementAdmissionFailureKind::Rollback(error),
                    parts: failure.parts,
                }),
            },
        }
    }

    pub(super) fn turn_one_acknowledgement_completion(
        &mut self,
    ) -> Result<ShareAcknowledgementCompletionTurn, ShareMembershipHostError> {
        if let Some(id) = self
            .acknowledgement_completions
            .next_reclaim()
            .map_err(|_error| ShareMembershipHostError::EffectShape)?
        {
            return match self.acknowledgement_completions.finish_reclaim(id) {
                Ok(ReclaimStatus::Reclaimed)
                | Err(CompletionRegistryError::GenerationExhausted) => {
                    Ok(ShareAcknowledgementCompletionTurn::Progress)
                }
                Ok(ReclaimStatus::Retry) => Ok(ShareAcknowledgementCompletionTurn::Blocked),
                Err(_error) => Err(ShareMembershipHostError::EffectShape),
            };
        }
        let Some((entry_index, session_index, owner)) = self.take_publication()? else {
            return Ok(ShareAcknowledgementCompletionTurn::Idle);
        };
        let (completion_id, terminal) = owner
            .into_publishable()
            .map_err(|_owner| ShareMembershipHostError::EffectShape)?;
        match self
            .acknowledgement_completions
            .publish(completion_id, terminal)
        {
            Ok(()) => Ok(ShareAcknowledgementCompletionTurn::Progress),
            Err((CompletionRegistryError::NotificationBackpressure, terminal)) => {
                let owner =
                    ShareAcknowledgementCompletionOwner::publishable(completion_id, terminal);
                self.entries[entry_index]
                    .fetch_mut()
                    .sessions_mut()
                    .ok_or(ShareMembershipHostError::EffectShape)?
                    .restore_acknowledgement_publication(session_index, owner)
                    .map_err(|_owner| ShareMembershipHostError::EffectShape)?;
                Ok(ShareAcknowledgementCompletionTurn::Blocked)
            }
            Err((_error, _terminal)) => Err(ShareMembershipHostError::EffectShape),
        }
    }

    fn take_publication(
        &mut self,
    ) -> Result<Option<(usize, usize, ShareAcknowledgementCompletionOwner)>, ShareMembershipHostError>
    {
        for (entry_index, entry) in self.entries.iter_mut().enumerate() {
            let Some(sessions) = entry.fetch_mut().sessions_mut() else {
                continue;
            };
            if let Some((session_index, owner)) = sessions
                .take_acknowledgement_publication()
                .map_err(|_error| ShareMembershipHostError::EffectShape)?
            {
                return Ok(Some((entry_index, session_index, owner)));
            }
        }
        Ok(None)
    }
}

fn acknowledgement_parts(
    acknowledgement: Box<kafka_client_core::ShareAcknowledgement>,
    completion: ShareAcknowledgementCompletionOwner,
) -> ShareAcknowledgementAdmissionParts {
    let Some((_id, recovery)) = completion.into_pending() else {
        unreachable!("new acknowledgement admission owns pending completion")
    };
    ShareAcknowledgementAdmissionParts {
        inner: acknowledgement,
        recovery,
    }
}
