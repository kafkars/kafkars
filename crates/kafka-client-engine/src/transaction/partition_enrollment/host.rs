//! Bounded identity, set, call, and terminal ownership for partition enrollment.

use std::sync::Arc;

use kafka_client_core::{
    Deadline, DeliveryStatus, ProducerRetryPolicy, TransactionEpoch, TransactionalProducerIdentity,
};

use crate::{
    clock::OperationDeadline, producer::materialization::TransactionalMaterializationBatch,
};

use super::{
    identity::TransactionPartitionEnrollmentIdentity,
    model::{
        TransactionPartitionEnrollmentLimits, TransactionPartitionEnrollmentStartError,
        TransactionPartitionEnrollmentTarget, TransactionPartitionEnrollmentTerminal,
    },
    port::TransactionPartitionEnrollmentPortCall,
};

pub(super) struct PendingEnrollment {
    pub(super) epoch: TransactionEpoch,
    pub(super) target: TransactionPartitionEnrollmentTarget,
    pub(super) deadline: OperationDeadline,
    pub(super) retry_not_before: Option<Deadline>,
    pub(super) retries_started: u32,
    pub(super) delivery_floor: DeliveryStatus,
    pub(super) batch: Option<TransactionalMaterializationBatch>,
    pub(super) call: Option<Box<dyn TransactionPartitionEnrollmentPortCall>>,
}

/// Private bounded owner for exact transactional topic-partition enrollment.
pub(crate) struct TransactionPartitionEnrollmentOwner {
    pub(super) identity: TransactionPartitionEnrollmentIdentity,
    pub(super) active_epoch: Option<TransactionEpoch>,
    pub(super) limits: TransactionPartitionEnrollmentLimits,
    pub(super) retry_policy: ProducerRetryPolicy,
    pub(super) enrolled: Vec<TransactionPartitionEnrollmentTarget>,
    pub(super) retained_topic_bytes: usize,
    pub(super) pending: Option<PendingEnrollment>,
    pub(super) terminal: Option<TransactionPartitionEnrollmentTerminal>,
}

impl TransactionPartitionEnrollmentOwner {
    /// Reserves one fixed enrolled set for an initialized producer identity.
    pub(crate) fn try_start(
        transactional_id: Arc<str>,
        producer: TransactionalProducerIdentity,
        limits: TransactionPartitionEnrollmentLimits,
        retry_policy: ProducerRetryPolicy,
    ) -> Result<Self, TransactionPartitionEnrollmentStartError> {
        if transactional_id.is_empty() {
            return Err(TransactionPartitionEnrollmentStartError::EmptyTransactionalId);
        }
        let mut enrolled = Vec::new();
        enrolled
            .try_reserve_exact(limits.max_partitions())
            .map_err(|_| TransactionPartitionEnrollmentStartError::RetainedBytes)?;
        Ok(Self {
            identity: TransactionPartitionEnrollmentIdentity::new(transactional_id, producer),
            active_epoch: None,
            limits,
            retry_policy,
            enrolled,
            retained_topic_bytes: 0,
            pending: None,
            terminal: None,
        })
    }

    /// Takes the sole settled terminal, reopening the next admission slot.
    pub(crate) fn take_terminal(&mut self) -> Option<TransactionPartitionEnrollmentTerminal> {
        self.terminal.take()
    }

    /// Returns the exact number of targets enrolled in the active epoch.
    #[cfg(test)]
    pub(crate) fn enrolled_partitions(&self) -> usize {
        self.enrolled.len()
    }

    /// Returns retained topic-name bytes charged to the active enrolled set.
    #[cfg(test)]
    pub(crate) fn retained_topic_bytes(&self) -> usize {
        self.retained_topic_bytes
    }

    /// Returns the original deadline while submission or causal refresh remains live.
    pub(crate) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        self.pending.as_ref().map(|pending| {
            if pending.call.is_some() {
                pending.deadline.core()
            } else {
                pending
                    .retry_not_before
                    .map_or(pending.deadline.core(), |not_before| {
                        not_before.min(pending.deadline.core())
                    })
            }
        })
    }

    /// Returns exact retained pending and terminal ownership.
    pub(crate) fn unsettled(&self) -> usize {
        usize::from(self.pending.is_some()) + usize::from(self.terminal.is_some())
    }

    /// Returns whether exact broker fencing has already settled locally.
    pub(crate) fn has_fatal_terminal(&self) -> bool {
        matches!(
            self.terminal.as_ref(),
            Some(TransactionPartitionEnrollmentTerminal::Fatal { .. })
        )
    }
}
