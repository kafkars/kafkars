//! Assigned-consumer orchestration of bounded broker Fetch-session cleanup.

use crate::driver::DriverOwner;

use super::super::{
    assigned_close_error::AssignedCloseSlotPhase, assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerOwnerFault,
};

impl AssignedConsumerOwner {
    pub(in crate::consumer) fn progress_broker_session_close(
        &mut self,
        driver: &DriverOwner,
    ) -> bool {
        if self.is_faulted() || self.close.phase() != AssignedCloseSlotPhase::Accepted {
            return false;
        }
        self.fetches.request_broker_session_close();
        let now = match self.clock.now() {
            Ok(now) => now,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Clock(error));
                return false;
            }
        };
        match self
            .fetches
            .drive_broker_session_close(driver, &self.clock, now)
        {
            Ok(progressed) => progressed,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                false
            }
        }
    }
}
