//! Earliest retained deadline across one broker-local share-fetch session.

use super::{
    fetch_acknowledgement::PreparedShareAcknowledgement,
    fetch_acknowledgement_execution::ActiveShareAcknowledgementCall,
    fetch_session::{PreparedShareFetchSession, ShareFetchSessionOwner},
    fetch_session_execution::ActiveShareFetchCall,
};

impl ShareFetchSessionOwner {
    pub(super) const fn throttle_until(&self) -> Option<kafka_client_core::Deadline> {
        self.throttle_until
    }

    pub(super) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        let execution = self
            .prepared
            .as_ref()
            .map(PreparedShareFetchSession::deadline)
            .or_else(|| self.active.as_ref().map(ActiveShareFetchCall::deadline));
        let acknowledgement = self
            .prepared_acknowledgement
            .as_ref()
            .map(PreparedShareAcknowledgement::deadline)
            .or_else(|| {
                self.active_acknowledgement
                    .as_ref()
                    .map(ActiveShareAcknowledgementCall::deadline)
            });
        let ledger = self.machine.ledger().next_reclaimable_deadline();
        let throttle = self
            .machine
            .ledger()
            .is_empty()
            .then_some(self.throttle_until)
            .flatten();
        [execution, acknowledgement, ledger, throttle]
            .into_iter()
            .flatten()
            .min()
    }
}
