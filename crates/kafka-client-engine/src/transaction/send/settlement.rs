//! Enrollment and Produce settlement with retained exact evidence.

use kafka_client_core::{DeliveryStatus, TransactionSendOutcome};

use crate::transaction::{
    TransactionLifecycleHostError, partition_enrollment::TransactionPartitionEnrollmentTerminal,
};

use super::{
    aggregate::TransactionSendAggregate,
    model::{TransactionSendFailure, TransactionSendFailureKind},
    owner::TransactionSendOwner,
    partitioning::TransactionPartitioningFailure,
    terminal::{failure, failure_terminal},
    turn::{PendingTransactionPartitioning, PendingTransactionSend, TransactionSendSlot},
};

impl TransactionSendOwner {
    pub(super) fn finish_partitioning(
        &mut self,
        pending: PendingTransactionPartitioning,
        failure: TransactionPartitioningFailure,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        let outcome = if failure == TransactionPartitioningFailure::TopicIdentityMismatch {
            TransactionSendOutcome::AbortRequired
        } else {
            TransactionSendOutcome::FailedHealthy
        };
        self.finish_unsequenced(
            pending,
            TransactionSendFailure::new(
                TransactionSendFailureKind::Partitioning(failure),
                DeliveryStatus::NotSent,
            ),
            outcome,
            lifecycle,
        )
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "settlement consumes the exact pending partitioning owner"
    )]
    pub(super) fn finish_unsequenced(
        &mut self,
        pending: PendingTransactionPartitioning,
        failure: TransactionSendFailure,
        outcome: TransactionSendOutcome,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        lifecycle.settle_unsequenced_send(pending.epoch, pending.send_id, outcome)?;
        let terminal = match outcome {
            TransactionSendOutcome::FailedHealthy => {
                super::model::TransactionSendTerminal::FailedHealthy {
                    epoch: pending.epoch,
                    send_id: pending.send_id,
                    failure,
                }
            }
            TransactionSendOutcome::AbortRequired => {
                super::model::TransactionSendTerminal::AbortRequired {
                    epoch: pending.epoch,
                    send_id: pending.send_id,
                    failure,
                }
            }
            TransactionSendOutcome::Fatal => super::model::TransactionSendTerminal::Fatal {
                epoch: pending.epoch,
                send_id: pending.send_id,
                failure,
            },
            TransactionSendOutcome::Succeeded => {
                unreachable!("failed partition lookup cannot succeed")
            }
        };
        self.slot = TransactionSendSlot::Terminal(pending.completion_id, terminal);
        Ok(())
    }

    pub(super) fn settle_enrollment(
        &mut self,
        pending: PendingTransactionSend,
        terminal: TransactionPartitionEnrollmentTerminal,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) {
        match terminal {
            TransactionPartitionEnrollmentTerminal::Enrolled(fence)
                if fence.epoch() == pending.epoch =>
            {
                self.slot = TransactionSendSlot::Ready(pending, fence.into_batch());
            }
            TransactionPartitionEnrollmentTerminal::Enrolled(fence) => {
                drop(fence.into_batch());
                self.finish_unproduced_infallible(
                    pending,
                    TransactionSendOutcome::Fatal,
                    failure(
                        TransactionSendFailureKind::Correlation,
                        DeliveryStatus::PossiblySent,
                    ),
                    lifecycle,
                );
            }
            TransactionPartitionEnrollmentTerminal::Rejected(rejection) => {
                let kind = rejection.kind();
                drop(rejection.into_batch());
                self.finish_unproduced_infallible(
                    pending,
                    TransactionSendOutcome::FailedHealthy,
                    failure(
                        TransactionSendFailureKind::Enrollment(kind),
                        DeliveryStatus::NotSent,
                    ),
                    lifecycle,
                );
            }
            TransactionPartitionEnrollmentTerminal::AbortRequired {
                kind,
                delivery,
                batch,
            } => {
                drop(batch);
                self.finish_unproduced_infallible(
                    pending,
                    TransactionSendOutcome::AbortRequired,
                    failure(TransactionSendFailureKind::Enrollment(kind), delivery),
                    lifecycle,
                );
            }
            TransactionPartitionEnrollmentTerminal::Fatal { kind, batch } => {
                drop(batch);
                self.finish_unproduced_infallible(
                    pending,
                    TransactionSendOutcome::Fatal,
                    failure(
                        TransactionSendFailureKind::Enrollment(kind),
                        DeliveryStatus::PossiblySent,
                    ),
                    lifecycle,
                );
            }
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "settlement consumes the exact pending send owner"
    )]
    pub(super) fn finish_unproduced(
        &mut self,
        pending: PendingTransactionSend,
        outcome: TransactionSendOutcome,
        failure: TransactionSendFailure,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) -> Result<(), TransactionLifecycleHostError> {
        lifecycle.settle_unproduced(
            pending.epoch,
            pending.send_id,
            pending.partition,
            pending.sequence,
            outcome,
        )?;
        self.slot = TransactionSendSlot::Terminal(
            pending.completion_id,
            failure_terminal(&pending, outcome, failure),
        );
        Ok(())
    }

    fn finish_unproduced_infallible(
        &mut self,
        pending: PendingTransactionSend,
        outcome: TransactionSendOutcome,
        failure: TransactionSendFailure,
        lifecycle: &mut dyn TransactionSendAggregate,
    ) {
        self.finish_unproduced(pending, outcome, failure, lifecycle)
            .unwrap_or_else(|_| unreachable!("exact unproduced send settlement is preflighted"));
    }
}
