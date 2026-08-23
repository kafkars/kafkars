//! Exact broker-session readiness after every fetch and acknowledgement owner settles.

use kafka_client_core::{Moment, ShareFetchSessionPhase};

use super::fetch_session::ShareFetchSessionOwner;

impl ShareFetchSessionOwner {
    pub(super) fn ready_for_preparation(&self, now: Moment) -> bool {
        self.prepared.is_none()
            && self.active.is_none()
            && self.terminal.is_none()
            && self.staged.is_none()
            && self.prepared_acknowledgement.is_none()
            && self.active_acknowledgement.is_none()
            && self.acknowledgement_terminal.is_none()
            && self.acknowledgement_outcome.is_none()
            && self.acknowledgement_completion.is_none()
            && self.acknowledgement_faults.is_empty()
            && self.machine.ledger().is_empty()
            && self.machine.phase() == ShareFetchSessionPhase::Ready
            && self
                .throttle_until
                .is_none_or(|deadline| deadline.is_elapsed_at(now))
    }
}
