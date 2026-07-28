//! Driver submission, terminal polling, and deterministic enrollment settlement.

use kafka_client_core::{DeliveryStatus, Moment, TransactionEpoch};

use super::{
    host::TransactionPartitionEnrollmentOwner,
    model::{
        TransactionPartitionEnrollmentAdmissionFailure, TransactionPartitionEnrollmentFailureKind,
        TransactionPartitionEnrollmentFence, TransactionPartitionEnrollmentTerminal,
        TransactionPartitionEnrollmentTurn,
    },
    port::{
        TransactionPartitionEnrollmentPort, TransactionPartitionEnrollmentPortCallPoll,
        TransactionPartitionEnrollmentPortEvidence, TransactionPartitionEnrollmentPortFact,
        TransactionPartitionEnrollmentRequest,
    },
};

impl TransactionPartitionEnrollmentOwner {
    #[cfg(test)]
    pub(in crate::transaction) fn settle_pending_enrolled_for_test(&mut self) {
        self.settle_accepted(TransactionPartitionEnrollmentPortFact::Enrolled);
    }

    /// Reconciles the sole pending batch after driver shutdown.
    pub(crate) fn recover_after_driver_shutdown(&mut self) {
        let Some(mut pending) = self.pending.take() else {
            return;
        };
        let accepted = pending.call.is_some();
        if let Some(call) = pending.call.take() {
            call.discard_after_driver_shutdown();
        }
        let batch = pending
            .batch
            .take()
            .unwrap_or_else(|| unreachable!("pending enrollment retains its exact batch"));
        self.terminal = Some(if accepted {
            TransactionPartitionEnrollmentTerminal::AbortRequired {
                kind: TransactionPartitionEnrollmentFailureKind::DriverClosed,
                delivery: DeliveryStatus::PossiblySent,
                batch,
            }
        } else {
            rejected(
                TransactionPartitionEnrollmentFailureKind::DriverRejected,
                batch,
            )
        });
    }

    pub(super) fn turn_with(
        &mut self,
        now: Moment,
        port: &mut dyn TransactionPartitionEnrollmentPort,
    ) -> TransactionPartitionEnrollmentTurn {
        if self.terminal.is_some() {
            return TransactionPartitionEnrollmentTurn::Idle;
        }
        let Some(pending) = self.pending.as_mut() else {
            return TransactionPartitionEnrollmentTurn::Idle;
        };
        if pending.call.is_none() {
            if pending.deadline.core().is_elapsed_at(now) {
                self.reject_pending(TransactionPartitionEnrollmentFailureKind::DeadlineElapsed);
            } else if pending
                .retry_not_before
                .is_some_and(|not_before| !not_before.is_elapsed_at(now))
            {
                return TransactionPartitionEnrollmentTurn::Idle;
            } else {
                pending.retry_not_before = None;
                let request = TransactionPartitionEnrollmentRequest {
                    epoch: pending.epoch,
                    transactional_id: self.identity.transactional_id(),
                    producer_id: self.identity.producer().producer_id(),
                    producer_epoch: self.identity.producer().producer_epoch(),
                    topic: pending.target.topic(),
                    partition: pending.target.partition(),
                    deadline: pending.deadline.transport(),
                };
                match port.submit(request) {
                    Ok(call) => pending.call = Some(call),
                    Err(()) => self
                        .reject_pending(TransactionPartitionEnrollmentFailureKind::DriverRejected),
                }
            }
            return TransactionPartitionEnrollmentTurn::Progress;
        }
        let (evidence, deadline_elapsed) = match pending
            .call
            .as_mut()
            .unwrap_or_else(|| unreachable!("checked accepted enrollment call"))
            .poll(pending.deadline.core().is_elapsed_at(now))
        {
            TransactionPartitionEnrollmentPortCallPoll::Pending => {
                return TransactionPartitionEnrollmentTurn::Idle;
            }
            TransactionPartitionEnrollmentPortCallPoll::Progress => {
                return TransactionPartitionEnrollmentTurn::Progress;
            }
            TransactionPartitionEnrollmentPortCallPoll::DeadlineElapsed(evidence) => {
                (evidence, true)
            }
            TransactionPartitionEnrollmentPortCallPoll::Terminal(evidence) => (evidence, false),
        };
        let fact = evidence_fact(pending.epoch, evidence.as_ref(), deadline_elapsed);
        self.settle_accepted_at(now, fact);
        evidence.discard();
        TransactionPartitionEnrollmentTurn::Progress
    }

    #[cfg(test)]
    fn settle_accepted(&mut self, fact: TransactionPartitionEnrollmentPortFact) {
        self.settle_accepted_at(Moment::from_tick(0), fact);
    }

    fn settle_accepted_at(&mut self, now: Moment, fact: TransactionPartitionEnrollmentPortFact) {
        let fact = match fact {
            TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss { .. }
                if self.schedule_retry(now) =>
            {
                return;
            }
            TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss { kind, delivery } => {
                TransactionPartitionEnrollmentPortFact::Failed { kind, delivery }
            }
            fact => fact,
        };
        let mut pending = self
            .pending
            .take()
            .unwrap_or_else(|| unreachable!("terminal fact requires one pending enrollment"));
        let batch = pending
            .batch
            .take()
            .unwrap_or_else(|| unreachable!("pending enrollment retains its exact batch"));
        self.terminal = Some(match fact {
            TransactionPartitionEnrollmentPortFact::Enrolled => {
                self.retained_topic_bytes += pending.target.retained_topic_bytes();
                self.enrolled.push(pending.target);
                TransactionPartitionEnrollmentTerminal::Enrolled(
                    TransactionPartitionEnrollmentFence::new(pending.epoch, batch),
                )
            }
            TransactionPartitionEnrollmentPortFact::Failed { kind, .. } if kind.is_fatal() => {
                TransactionPartitionEnrollmentTerminal::Fatal { kind, batch }
            }
            TransactionPartitionEnrollmentPortFact::Failed {
                kind,
                delivery: DeliveryStatus::NotSent,
            } => rejected(kind, batch),
            TransactionPartitionEnrollmentPortFact::Failed { kind, delivery } => {
                TransactionPartitionEnrollmentTerminal::AbortRequired {
                    kind,
                    delivery,
                    batch,
                }
            }
            TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss { .. } => {
                unreachable!("retryable coordinator loss is normalized before settlement")
            }
        });
    }

    fn schedule_retry(&mut self, now: Moment) -> bool {
        let Some(pending) = self.pending.as_mut() else {
            return false;
        };
        if pending.retries_started >= self.retry_policy.max_retries()
            || pending.deadline.core().is_elapsed_at(now)
        {
            return false;
        }
        let Some(not_before) = now.checked_deadline_after(self.retry_policy.backoff_ticks()) else {
            return false;
        };
        if not_before >= pending.deadline.core() {
            return false;
        }
        let Some(retries_started) = pending.retries_started.checked_add(1) else {
            return false;
        };
        drop(pending.call.take());
        pending.retry_not_before = Some(not_before);
        pending.retries_started = retries_started;
        true
    }

    fn reject_pending(&mut self, kind: TransactionPartitionEnrollmentFailureKind) {
        let mut pending = self
            .pending
            .take()
            .unwrap_or_else(|| unreachable!("local rejection requires one pending enrollment"));
        let batch = pending
            .batch
            .take()
            .unwrap_or_else(|| unreachable!("pending enrollment retains its exact batch"));
        self.terminal = Some(rejected(kind, batch));
    }
}

fn evidence_fact(
    epoch: TransactionEpoch,
    evidence: &dyn TransactionPartitionEnrollmentPortEvidence,
    deadline_elapsed: bool,
) -> TransactionPartitionEnrollmentPortFact {
    if evidence.epoch() != epoch {
        return TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::InvalidResponse,
            delivery: DeliveryStatus::PossiblySent,
        };
    }
    match (deadline_elapsed, evidence.fact()) {
        (false, fact) => fact,
        (
            true,
            TransactionPartitionEnrollmentPortFact::RetryableCoordinatorLoss { delivery, .. }
            | TransactionPartitionEnrollmentPortFact::Failed { delivery, .. },
        ) => TransactionPartitionEnrollmentPortFact::Failed {
            kind: TransactionPartitionEnrollmentFailureKind::DeadlineElapsed,
            delivery,
        },
        (true, TransactionPartitionEnrollmentPortFact::Enrolled) => {
            TransactionPartitionEnrollmentPortFact::Failed {
                kind: TransactionPartitionEnrollmentFailureKind::InvalidResponse,
                delivery: DeliveryStatus::PossiblySent,
            }
        }
    }
}

const fn rejected(
    kind: TransactionPartitionEnrollmentFailureKind,
    batch: crate::producer::materialization::TransactionalMaterializationBatch,
) -> TransactionPartitionEnrollmentTerminal {
    TransactionPartitionEnrollmentTerminal::Rejected(
        TransactionPartitionEnrollmentAdmissionFailure::new(kind, batch),
    )
}
