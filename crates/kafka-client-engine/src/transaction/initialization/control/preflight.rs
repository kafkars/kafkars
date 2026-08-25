//! Commit preflight forwarding through the initialized transaction control port.

use kafka_client_core::{TransactionEpoch, TransactionalOwnerId};

use super::{TransactionLifecycleControlError, TransactionLifecycleControlPort};

impl TransactionLifecycleControlPort {
    pub(crate) fn preflight_commit(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
    ) -> Result<(), TransactionLifecycleControlError> {
        self.shared.try_preflight_commit(owner_id, epoch)
    }
}
