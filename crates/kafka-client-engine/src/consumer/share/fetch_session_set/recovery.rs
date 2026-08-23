//! Driver-shutdown recovery for every exact session-set owner.

use super::{
    super::fetch_session_execution::ShareFetchExecutionError,
    owner::{ShareFetchSessionSet, release_unsubmitted},
};

impl ShareFetchSessionSet {
    pub(in crate::consumer::share) fn recover_after_driver_shutdown(
        mut self,
    ) -> Result<(), ShareFetchExecutionError> {
        for session in &mut self.sessions {
            let _recovered_acknowledgement =
                session.recover_acknowledgement_after_driver_shutdown()?;
            if session.acknowledgement_terminal.is_some() {
                let outcome = session.settle_acknowledgement_terminal()?;
                session
                    .retain_settled_acknowledgement(outcome)
                    .map_err(|_outcome| ShareFetchExecutionError::Occupied)?;
            }
            let _abandoned_outcome = session.abandon_acknowledgement_outcome()?;
            let _abandoned_prepared = session.abandon_prepared_acknowledgement()?;
            let _recovered = session.recover_call_after_driver_shutdown()?;
            let _discarded = session.discard_terminal()?;
        }
        release_unsubmitted(self.sessions)
    }
}
