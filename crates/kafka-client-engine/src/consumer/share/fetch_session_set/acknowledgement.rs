//! Acknowledgement-priority scheduling across one broker-session set.

use kafka_client_core::Moment;

use crate::{
    clock::DeadlineCapture,
    consumer::{
        share_acknowledge::{ShareAcknowledgementCompletionOwner, public_outcome},
        share_batch::ShareAcknowledgementAdmissionParts,
    },
    driver::DriverOwner,
};

use super::{
    super::{
        fetch_acknowledgement_execution::{
            ShareAcknowledgementExecutionPoll, ShareAcknowledgementSubmissionTurn,
        },
        fetch_session_execution::ShareFetchExecutionError,
    },
    owner::{ShareFetchSessionSet, ShareFetchSessionSetTurn},
};

#[must_use = "rejected session admission retains exact acknowledgement ownership"]
pub(in crate::consumer::share) struct ShareSessionAcknowledgementAdmissionFailure {
    pub(in crate::consumer::share) kind: ShareSessionAcknowledgementAdmissionFailureKind,
    pub(in crate::consumer::share) parts: ShareAcknowledgementAdmissionParts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::share) enum ShareSessionAcknowledgementAdmissionFailureKind {
    UnknownSession,
    Occupied,
    Preparation(super::super::fetch_acknowledgement::ShareAcknowledgementPreparationFailureKind),
}

impl ShareFetchSessionSet {
    pub(in crate::consumer::share) fn prepare_public_acknowledgement(
        &mut self,
        acknowledgement: Box<kafka_client_core::ShareAcknowledgement>,
        capture: DeadlineCapture,
        completion: ShareAcknowledgementCompletionOwner,
    ) -> Result<(), ShareSessionAcknowledgementAdmissionFailure> {
        let fence = acknowledgement.fence();
        let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.owns_delivery_fence(fence))
        else {
            return Err(admission_failure(
                ShareSessionAcknowledgementAdmissionFailureKind::UnknownSession,
                acknowledgement,
                completion,
            ));
        };
        if session.acknowledgement_completion_is_present()
            || session.acknowledgement_outcome.is_some()
            || !session.acknowledgement_faults.is_empty()
        {
            return Err(admission_failure(
                ShareSessionAcknowledgementAdmissionFailureKind::Occupied,
                acknowledgement,
                completion,
            ));
        }
        match session.prepare_acknowledgement(*acknowledgement, capture, capture.now()) {
            Ok(()) => {
                session
                    .install_acknowledgement_completion(completion)
                    .unwrap_or_else(|_completion| {
                        unreachable!("validated acknowledgement completion slot")
                    });
                Ok(())
            }
            Err(failure) => Err(admission_failure(
                ShareSessionAcknowledgementAdmissionFailureKind::Preparation(failure.kind),
                Box::new(failure.acknowledgement),
                completion,
            )),
        }
    }

    pub(in crate::consumer::share) fn take_acknowledgement_publication(
        &mut self,
    ) -> Result<Option<(usize, ShareAcknowledgementCompletionOwner)>, ShareFetchExecutionError>
    {
        for (index, session) in self.sessions.iter_mut().enumerate() {
            if session.acknowledgement_completion_is_publishable() {
                return Ok(session
                    .take_acknowledgement_completion()
                    .map(|owner| (index, owner)));
            }
            let Some(outcome) = session.take_acknowledgement_outcome() else {
                continue;
            };
            let owner = session
                .take_acknowledgement_completion()
                .ok_or(ShareFetchExecutionError::Occupied)?;
            let Some((id, recovery)) = owner.into_pending() else {
                return Err(ShareFetchExecutionError::Occupied);
            };
            let terminal = public_outcome(outcome, recovery);
            return Ok(Some((
                index,
                ShareAcknowledgementCompletionOwner::publishable(id, terminal),
            )));
        }
        Ok(None)
    }

    pub(in crate::consumer::share) fn restore_acknowledgement_publication(
        &mut self,
        index: usize,
        owner: ShareAcknowledgementCompletionOwner,
    ) -> Result<(), ShareAcknowledgementCompletionOwner> {
        let Some(session) = self.sessions.get_mut(index) else {
            return Err(owner);
        };
        if session.acknowledgement_completion_is_present() {
            return Err(owner);
        }
        session.install_acknowledgement_completion(owner)
    }

    pub(super) fn turn_acknowledgement(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> Result<(Option<ShareFetchSessionSetTurn>, bool), ShareFetchExecutionError> {
        for session in &mut self.sessions {
            if session.acknowledgement_terminal.is_some() {
                let outcome = session
                    .settle_acknowledgement_terminal()
                    .map_err(ShareFetchExecutionError::Acknowledgement)?;
                session
                    .retain_settled_acknowledgement(outcome)
                    .map_err(|_outcome| ShareFetchExecutionError::Occupied)?;
                return Ok((Some(ShareFetchSessionSetTurn::Progress), false));
            }
        }
        let mut active = false;
        for session in &mut self.sessions {
            if session.active_acknowledgement.is_none() {
                continue;
            }
            active = true;
            match session
                .poll_acknowledgement()
                .map_err(ShareFetchExecutionError::Acknowledgement)?
            {
                ShareAcknowledgementExecutionPoll::Pending => {}
                ShareAcknowledgementExecutionPoll::Terminal => {
                    return Ok((Some(ShareFetchSessionSetTurn::Progress), false));
                }
            }
        }
        if let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.prepared_acknowledgement.is_some())
        {
            let turn = match session
                .submit_prepared_acknowledgement(driver, now)
                .map_err(ShareFetchExecutionError::Acknowledgement)?
            {
                ShareAcknowledgementSubmissionTurn::Submitted
                | ShareAcknowledgementSubmissionTurn::Terminal => {
                    ShareFetchSessionSetTurn::Progress
                }
                ShareAcknowledgementSubmissionTurn::Backpressured => {
                    ShareFetchSessionSetTurn::Blocked
                }
            };
            return Ok((Some(turn), false));
        }
        if self
            .sessions
            .iter()
            .any(|session| session.acknowledgement_outcome.is_some())
        {
            return Ok((Some(ShareFetchSessionSetTurn::Blocked), false));
        }
        Ok((None, active))
    }
}

fn admission_failure(
    kind: ShareSessionAcknowledgementAdmissionFailureKind,
    acknowledgement: Box<kafka_client_core::ShareAcknowledgement>,
    completion: ShareAcknowledgementCompletionOwner,
) -> ShareSessionAcknowledgementAdmissionFailure {
    let Some((_id, recovery)) = completion.into_pending() else {
        unreachable!("new acknowledgement admission owns pending completion")
    };
    ShareSessionAcknowledgementAdmissionFailure {
        kind,
        parts: ShareAcknowledgementAdmissionParts {
            inner: acknowledgement,
            recovery,
        },
    }
}
