//! Lock-bounded commit preflight against the exact initialized transaction owner.

use kafka_client_core::{TransactionEpoch, TransactionalOwnerId};

use super::TransactionInitializationShardState;
use crate::transaction::initialization::TransactionLifecycleControlError;

impl TransactionInitializationShardState {
    pub(in crate::transaction::initialization) fn try_preflight_commit(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionLifecycleControlError> {
        let host = self.try_control_host()?;
        host.preflight_commit_lifecycle(owner_id, epoch)
    }
}
