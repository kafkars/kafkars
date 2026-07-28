//! Narrow lifecycle identity and health port for transactional offsets.

use std::sync::Arc;

use kafka_client_core::{
    TransactionEpoch, TransactionOffsetCommitMachineError, TransactionalOwnerId,
    TransactionalProducerIdentity,
};

use crate::transaction::{
    completion::TransactionOffsetCommitPublisher, initialization::TransactionalOwnerParts,
};

use super::host::{TransactionLifecycleHost, TransactionLifecycleHostError};

impl TransactionLifecycleHost {
    pub(in crate::transaction) fn offset_commit_identity(
        &self,
        owner_id: TransactionalOwnerId,
        epoch: TransactionEpoch,
    ) -> Result<(Arc<str>, TransactionalProducerIdentity), TransactionLifecycleHostError> {
        if !self.owns(owner_id) {
            return Err(TransactionLifecycleHostError::UnexpectedEffect);
        }
        self.machine.preflight_offset_commit(epoch)?;
        let transactional_id = self
            .owner
            .as_ref()
            .and_then(TransactionalOwnerParts::transactional_id_arc)
            .ok_or(TransactionLifecycleHostError::UnexpectedEffect)?;
        Ok((transactional_id, self.producer_identity()?))
    }

    pub(in crate::transaction) fn take_offset_commit_publisher(
        &mut self,
    ) -> TransactionOffsetCommitPublisher {
        self.owner.as_mut().map_or_else(
            || unreachable!("new execution retains its owner parts"),
            TransactionalOwnerParts::take_offset_commit_publisher,
        )
    }
}

impl From<TransactionOffsetCommitMachineError> for TransactionLifecycleHostError {
    fn from(error: TransactionOffsetCommitMachineError) -> Self {
        Self::OffsetCommit(error)
    }
}
