//! Lifecycle execution turns and ordered post-driver recovery.

use kafka_client_core::Moment;

use crate::{
    driver::DriverOwner,
    transaction::{TransactionLifecycleHostError, TransactionLifecycleTurn},
};

use super::host::TransactionExecutionHost;

impl TransactionExecutionHost {
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        self.lifecycle.turn(now, driver)
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.lifecycle.recover_end_after_driver_shutdown()
    }
}
