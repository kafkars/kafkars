//! Test-only deterministic settlement helpers for transaction initialization.

use kafka_client_core::TransactionInitializationInput;

use super::TransactionInitializationHost;
use crate::transaction::initialization::TransactionInitializationHostError;

impl TransactionInitializationHost {
    pub(in crate::transaction::initialization) fn reclaim_for_test(
        &mut self,
    ) -> Result<bool, TransactionInitializationHostError> {
        self.reclaim_one()
    }

    pub(in crate::transaction::initialization) fn release_owner_for_test(
        &mut self,
    ) -> Result<bool, TransactionInitializationHostError> {
        self.release_one_owner()
    }

    pub(in crate::transaction::initialization) fn owner_loss_for_test(
        &mut self,
    ) -> Result<bool, TransactionInitializationHostError> {
        self.owner_loss_one()
    }

    pub(in crate::transaction::initialization) fn prune_closed_lifecycles_for_test(&mut self) {
        self.executions.retain(|execution| !execution.is_closed());
    }

    pub(in crate::transaction::initialization) const fn lifecycle_count_for_test(&self) -> usize {
        self.executions.len()
    }

    pub(in crate::transaction::initialization) fn initialize_for_test(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<(), TransactionInitializationHostError> {
        if self.operations.is_empty() {
            return Err(TransactionInitializationHostError::UnknownOperation);
        }
        self.apply(0, TransactionInitializationInput::DriverAccepted)?;
        self.apply(
            0,
            TransactionInitializationInput::BrokerInitialized {
                producer_id,
                producer_epoch,
            },
        )
    }

    pub(in crate::transaction::initialization) fn install_refresh_call_for_test(
        &mut self,
        driver: &super::DriverOwner,
        error_code: i16,
    ) -> Result<(), TransactionInitializationHostError> {
        let operation = self
            .operations
            .first_mut()
            .ok_or(TransactionInitializationHostError::UnknownOperation)?;
        operation.call = Some(super::TransactionInitCall::refreshing_for_test(
            driver, error_code,
        ));
        self.apply(0, TransactionInitializationInput::DriverAccepted)
    }
}
