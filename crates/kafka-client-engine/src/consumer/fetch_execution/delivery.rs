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
    pub(crate) fn recover_after_driver_shutdown(mut self) -> FetchShutdownRecovery {
        let driver = self.calls.recover_fetches_after_driver_shutdown();
        FetchShutdownRecovery::new(driver, self.fault.is_some())
    }
}
