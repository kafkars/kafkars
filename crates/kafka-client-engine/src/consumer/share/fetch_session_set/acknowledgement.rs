//! Acknowledgement-priority scheduling across one broker-session set.

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::{
    super::{
        fetch_acknowledgement_execution::{
            ShareAcknowledgementExecutionPoll, ShareAcknowledgementSubmissionTurn,
        },
        fetch_session_execution::ShareFetchExecutionError,
    },
    owner::{ShareFetchSessionSet, ShareFetchSessionSetTurn},
};

impl ShareFetchSessionSet {
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
