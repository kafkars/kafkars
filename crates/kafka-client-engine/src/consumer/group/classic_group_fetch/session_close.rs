//! Retirement-triggered cleanup of classic-group broker Fetch sessions.

use crate::{clock::MonotonicClock, driver::DriverOwner};

use super::{
    model::ClassicGroupFetchOwnerFault, owner::ClassicGroupFetchOwner,
    turn_model::ClassicGroupFetchTurn,
};

impl ClassicGroupFetchOwner {
    pub(super) fn close_retired_broker_sessions(
        &mut self,
        clock: &MonotonicClock,
        driver: &DriverOwner,
        work: &mut ClassicGroupFetchTurn,
    ) {
        if self.is_faulted() || !self.fetches.broker_session_close_requested() {
            return;
        }
        let now = match clock.now() {
            Ok(now) => now,
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Clock(error));
                self.settle_seek_host_unavailable();
                work.fault_retained = true;
                return;
            }
        };
        match self.fetches.drive_broker_session_close(driver, clock, now) {
            Ok(progressed) => work.fetch_polled |= progressed,
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                self.settle_seek_host_unavailable();
                work.fault_retained = true;
            }
        }
    }
}
