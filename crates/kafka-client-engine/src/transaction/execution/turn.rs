//! Send-first execution turns and ordered post-driver recovery.

use kafka_client_core::Moment;

use crate::{
    driver::DriverOwner,
    transaction::{
        TransactionLifecycleHostError, TransactionLifecycleTurn,
        offset_commit::{TransactionOffsetCommitHostError, TransactionOffsetCommitTurn},
        send::TransactionSendTurn,
    },
};

use super::host::TransactionExecutionHost;

impl TransactionExecutionHost {
    pub(crate) fn turn(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        if self.send.turn(&mut self.lifecycle, now, driver)? == TransactionSendTurn::Progress {
            return Ok(TransactionLifecycleTurn::Progress);
        }
        if self
            .offset_commit
            .turn(&mut self.lifecycle, now, driver)
            .map_err(offset_error)?
            == TransactionOffsetCommitTurn::Progress
        {
            return Ok(TransactionLifecycleTurn::Progress);
        }
        if self.offset_commit.has_unsettled_barrier() {
            return Ok(TransactionLifecycleTurn::Idle);
        }
        if let Some(deadline) = self.owner_loss_pending.take() {
            self.lifecycle.owner_lost(deadline)?;
            return Ok(TransactionLifecycleTurn::Progress);
        }
        self.lifecycle.turn(now, driver)
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), TransactionLifecycleHostError> {
        self.send
            .recover_after_driver_shutdown(&mut self.lifecycle)?;
        self.offset_commit
            .recover_after_driver_shutdown(&mut self.lifecycle)
            .map_err(offset_error)?;
        self.send.publish_terminal_after_driver_shutdown()?;
        self.offset_commit
            .publish_terminal_after_driver_shutdown()
            .map_err(offset_error)?;
        if let Some(deadline) = self.owner_loss_pending.take() {
            self.lifecycle.owner_lost(deadline)?;
        }
        self.lifecycle.recover_end_after_driver_shutdown()
    }

    #[cfg(test)]
    pub(super) fn turn_with_produce_port_for_test(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
        port: &mut dyn crate::transaction::send::TransactionSendProducePort,
    ) -> Result<TransactionLifecycleTurn, TransactionLifecycleHostError> {
        if self
            .send
            .turn_with(&mut self.lifecycle, now, driver, port)?
            == TransactionSendTurn::Progress
        {
            return Ok(TransactionLifecycleTurn::Progress);
        }
        self.lifecycle.turn(now, driver)
    }
}

const fn offset_error(_error: TransactionOffsetCommitHostError) -> TransactionLifecycleHostError {
    TransactionLifecycleHostError::UnexpectedEffect
}
