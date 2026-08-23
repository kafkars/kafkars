//! Fair execution, shutdown recovery, and lossless release for one session set.

use kafka_client_core::Moment;

use crate::driver::DriverOwner;

use super::{
    super::fetch_acknowledgement_execution::ShareAcknowledgementExecutionPoll,
    super::fetch_session_execution::{
        ShareFetchExecutionError, ShareFetchExecutionPoll, ShareFetchSubmissionTurn,
    },
    owner::{ShareFetchSessionSet, ShareFetchSessionSetTurn, release_unsubmitted},
};

impl ShareFetchSessionSet {
    pub(in crate::consumer::share) fn turn(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> Result<ShareFetchSessionSetTurn, ShareFetchExecutionError> {
        let (acknowledgement_turn, acknowledgement_active) =
            self.turn_acknowledgement(driver, now)?;
        if let Some(turn) = acknowledgement_turn {
            return Ok(turn);
        }
        for session in &mut self.sessions {
            if session.terminal.is_some() {
                session
                    .settle_terminal(now)
                    .map_err(|error| ShareFetchExecutionError::Settlement(error.kind()))?;
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
        }
        for session in &mut self.sessions {
            if session.has_staged_delivery()
                && session.machine().ledger().is_empty()
                && session
                    .discard_staged_delivery()
                    .map_err(ShareFetchExecutionError::Acquisition)?
            {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
            if session
                .expire_one_reclaimable(now)
                .map_err(ShareFetchExecutionError::Acquisition)?
            {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
        }
        let mut active = false;
        for session in &mut self.sessions {
            if !session.has_active_call() {
                continue;
            }
            active = true;
            match session.poll_execution()? {
                ShareFetchExecutionPoll::Pending => {}
                ShareFetchExecutionPoll::Terminal => {
                    return Ok(ShareFetchSessionSetTurn::Progress);
                }
            }
        }
        if let Some(session) = self
            .sessions
            .iter_mut()
            .find(|session| session.has_prepared())
        {
            return match session.submit_prepared(driver, now)? {
                ShareFetchSubmissionTurn::Submitted => Ok(ShareFetchSessionSetTurn::Progress),
                ShareFetchSubmissionTurn::Backpressured => Ok(ShareFetchSessionSetTurn::Blocked),
            };
        }
        if let Some(index) = self
            .sessions
            .iter()
            .position(|session| session.ready_for_preparation(now))
        {
            return Ok(ShareFetchSessionSetTurn::NeedsPreparation(index));
        }
        let retained = self.sessions.iter().any(|session| {
            session.has_staged_delivery()
                || !session.machine().ledger().is_empty()
                || session.throttle_until().is_some()
        });
        Ok(if active || acknowledgement_active || retained {
            ShareFetchSessionSetTurn::Blocked
        } else {
            ShareFetchSessionSetTurn::Idle
        })
    }

    pub(in crate::consumer::share) fn abandon_turn(
        &mut self,
    ) -> Result<ShareFetchSessionSetTurn, ShareFetchExecutionError> {
        for session in &mut self.sessions {
            if session.acknowledgement_terminal.is_some() {
                let outcome = session
                    .settle_acknowledgement_terminal()
                    .map_err(ShareFetchExecutionError::Acknowledgement)?;
                session
                    .retain_settled_acknowledgement(outcome)
                    .map_err(|_outcome| ShareFetchExecutionError::Occupied)?;
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
            if session
                .abandon_acknowledgement_outcome()
                .map_err(ShareFetchExecutionError::Acknowledgement)?
            {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
        }
        for session in &mut self.sessions {
            if session.active_acknowledgement.is_none() {
                continue;
            }
            match session
                .poll_acknowledgement()
                .map_err(ShareFetchExecutionError::Acknowledgement)?
            {
                ShareAcknowledgementExecutionPoll::Pending => {
                    return Ok(ShareFetchSessionSetTurn::Blocked);
                }
                ShareAcknowledgementExecutionPoll::Terminal => {
                    return Ok(ShareFetchSessionSetTurn::Progress);
                }
            }
        }
        for session in &mut self.sessions {
            if session
                .abandon_prepared_acknowledgement()
                .map_err(ShareFetchExecutionError::Acknowledgement)?
            {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
        }
        for session in &mut self.sessions {
            if session.discard_terminal()? {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
            if session
                .retire_one_reclaimable()
                .map_err(ShareFetchExecutionError::Acquisition)?
            {
                return Ok(ShareFetchSessionSetTurn::Progress);
            }
        }
        let mut active = false;
        for session in &mut self.sessions {
            if !session.has_active_call() {
                continue;
            }
            active = true;
            match session.poll_execution() {
                Ok(ShareFetchExecutionPoll::Pending) => {}
                Ok(ShareFetchExecutionPoll::Terminal) | Err(_) => {
                    return Ok(ShareFetchSessionSetTurn::Progress);
                }
            }
        }
        if active {
            return Ok(ShareFetchSessionSetTurn::Blocked);
        }
        if self
            .sessions
            .iter()
            .any(|session| !session.machine().ledger().is_empty())
        {
            return Ok(ShareFetchSessionSetTurn::Blocked);
        }
        Ok(ShareFetchSessionSetTurn::Released)
    }

    pub(in crate::consumer::share) fn release_unsubmitted(
        self,
    ) -> Result<(), ShareFetchExecutionError> {
        release_unsubmitted(self.sessions)
    }
}
