//! Two-phase lifecycle construction before consuming the initialized owner.

use kafka_client_core::{
    OperationId, TransactionLifecycleMachine, TransactionSequenceMachine,
    TransactionalProducerIdentity,
};

use crate::{
    completion::CompletionRegistry,
    transaction::{
        initialization::TransactionalOwnerParts,
        partition_enrollment::TransactionPartitionEnrollmentOwner,
    },
};

use super::{
    host::{END_COMPLETION_CAPACITY, TransactionLifecycleHost, TransactionLifecycleHostError},
    limits::TransactionExecutionLimits,
};

impl TransactionLifecycleHost {
    #[allow(
        clippy::result_large_err,
        reason = "failed construction returns the exact initialized transactional owner for recovery"
    )]
    pub(in crate::transaction) fn try_new(
        mut owner: TransactionalOwnerParts,
        limits: TransactionExecutionLimits,
    ) -> Result<Self, (TransactionLifecycleHostError, TransactionalOwnerParts)> {
        let Some(producer) =
            TransactionalProducerIdentity::try_new(owner.producer_id(), owner.producer_epoch())
        else {
            return Err((
                TransactionLifecycleHostError::InvalidProducerIdentity,
                owner,
            ));
        };
        let enrollment_limits = match limits.enrollment() {
            Ok(limits) => limits,
            Err(error) => return Err((error, owner)),
        };
        let enrollment = match TransactionPartitionEnrollmentOwner::try_start(
            owner
                .transactional_id_arc()
                .unwrap_or_else(|| unreachable!("execution parts retain shared identity")),
            producer,
            enrollment_limits,
            limits.send_retry_policy(),
        ) {
            Ok(enrollment) => enrollment,
            Err(error) => {
                return Err((TransactionLifecycleHostError::EnrollmentStart(error), owner));
            }
        };
        let sequencing = match TransactionSequenceMachine::try_new(limits.partition_capacity()) {
            Ok(sequencing) => sequencing,
            Err(error) => return Err((TransactionLifecycleHostError::Sequencing(error), owner)),
        };
        let publisher = owner.take_lifecycle_publisher();
        let machine = TransactionLifecycleMachine::with_send_retry_policy(
            owner.owner_id(),
            limits.send_retry_policy(),
        );
        Ok(Self {
            owner: Some(owner),
            machine,
            enrollment,
            sequencing,
            completions: CompletionRegistry::with_publisher(END_COMPLETION_CAPACITY, publisher),
            next_operation_id: Some(OperationId::from_raw(1)),
            pending_end: None,
            end_retry_policy: limits.send_retry_policy(),
            release_after_end: false,
            reclaim_pending: None,
        })
    }
}
