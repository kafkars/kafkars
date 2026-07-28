//! Two-phase lifecycle construction before consuming the initialized owner.

use kafka_client_core::{
    OperationId, ProducerRetryPolicy, TransactionLifecycleMachine, TransactionalProducerIdentity,
};

use crate::{completion::CompletionRegistry, transaction::initialization::TransactionalOwnerParts};

use super::host::{
    END_COMPLETION_CAPACITY, TransactionLifecycleHost, TransactionLifecycleHostError,
};

impl TransactionLifecycleHost {
    pub(in crate::transaction) fn try_new(
        mut owner: TransactionalOwnerParts,
        end_retry_policy: ProducerRetryPolicy,
    ) -> Result<Self, (TransactionLifecycleHostError, TransactionalOwnerParts)> {
        if TransactionalProducerIdentity::try_new(owner.producer_id(), owner.producer_epoch())
            .is_none()
        {
            return Err((
                TransactionLifecycleHostError::InvalidProducerIdentity,
                owner,
            ));
        }
        let publisher = owner.take_lifecycle_publisher();
        let machine = TransactionLifecycleMachine::new(owner.owner_id());
        Ok(Self {
            owner: Some(owner),
            machine,
            completions: CompletionRegistry::with_publisher(END_COMPLETION_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            pending_end: None,
            end_retry_policy,
            release_after_end: false,
            reclaim_pending: None,
        })
    }
}
