//! Application delivery lease access and post-driver shutdown release.

use super::{
    super::fetch_store::FetchDelivery,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, FetchReclaimFailure, FetchShutdownRecovery, RetainedFetchFault},
};

impl DirectFetchExecutor {
    pub(crate) fn take_ready(&mut self) -> Result<Option<FetchDelivery>, FetchExecutionError> {
        if self.fault.is_some() {
            return Err(FetchExecutionError::Faulted);
        }
        match self.store.take_ready() {
            Ok(delivery) => Ok(delivery),
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Staged);
                Err(FetchExecutionError::Store(error))
            }
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed reclamation returns the exact linear delivery lease"
    )]
    pub(crate) fn reclaim(&mut self, delivery: FetchDelivery) -> Result<(), FetchReclaimFailure> {
        if self.fault.is_some() {
            return Err(FetchReclaimFailure::new(
                FetchExecutionError::Faulted,
                delivery,
            ));
        }
        match self.store.reclaim(delivery) {
            Ok(()) => Ok(()),
            Err((error, delivery)) => Err(FetchReclaimFailure::new(
                FetchExecutionError::Store(error),
                delivery,
            )),
        }
    }

    /// Releases retained fault and store ownership only after driver shutdown.
    pub(crate) fn release_fetch_executor_after_driver_shutdown(mut self) -> FetchShutdownRecovery {
        let driver = self.calls.recover_fetches_after_driver_shutdown();
        let (mut requests, completion) = driver.into_parts();
        let broker = self.broker_calls.recover_after_driver_shutdown();
        let (broker_requests, broker_completion) = broker.into_parts();
        requests.extend(broker_requests);
        requests.extend(
            self.route_calls
                .drain(..)
                .map(|pending| pending.call.recover_after_driver_shutdown()),
        );
        requests.extend(self.routed.drain(..).map(|routed| routed.request));
        self.active_broker_sessions.clear();
        self.release_forgotten_maintenance_after_driver_shutdown();
        let driver = crate::driver::FetchRecovery::new(requests, completion.or(broker_completion));
        FetchShutdownRecovery::new(driver, self.fault.is_some())
    }
}
