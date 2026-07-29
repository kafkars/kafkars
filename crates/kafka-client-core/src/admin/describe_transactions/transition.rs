//! Atomic ID iteration and terminal assignment for Admin `DescribeTransactions`.

use crate::DeliveryStatus;

use super::{
    AdminDescribeTransactionOutcome, AdminDescribeTransactionsBatch,
    AdminDescribeTransactionsEffect, AdminDescribeTransactionsFailure,
    AdminDescribeTransactionsFailureKind, AdminDescribeTransactionsInput,
    AdminDescribeTransactionsMachine, AdminDescribeTransactionsMachineError,
    AdminDescribeTransactionsState, AdminDescribeTransactionsTerminal,
    AdminDescribeTransactionsTransition,
    normalization::{RetainedCounts, normalize_outcome},
};

impl AdminDescribeTransactionsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminDescribeTransactionsInput,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        if self.state == AdminDescribeTransactionsState::Completed {
            return Err(AdminDescribeTransactionsMachineError::AlreadyCompleted);
        }
        match input {
            AdminDescribeTransactionsInput::Start { now } => self.start(now),
            AdminDescribeTransactionsInput::DriverAccepted => self.driver_accepted(),
            AdminDescribeTransactionsInput::DriverRejected => self.finish_awaiting(
                AdminDescribeTransactionsFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            AdminDescribeTransactionsInput::DeadlineElapsed => self.finish_awaiting(
                AdminDescribeTransactionsFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            AdminDescribeTransactionsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AdminDescribeTransactionsFailureKind::DeadlineElapsed,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeTransactionsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminDescribeTransactionsInput::ResponseTooLarge => self.finish_submitted(
                AdminDescribeTransactionsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminDescribeTransactionsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AdminDescribeTransactionsFailureKind::Compatibility,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeTransactionsInput::TransportFailed { delivery } => self.finish_submitted(
                AdminDescribeTransactionsFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            AdminDescribeTransactionsInput::InvalidResponse => self.finish_submitted(
                AdminDescribeTransactionsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        if self.state != AdminDescribeTransactionsState::Ready {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminDescribeTransactionsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        let Some(transactional_id) = self
            .plan
            .transactional_ids()
            .get(self.next_transaction)
            .cloned()
        else {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        };
        self.state = AdminDescribeTransactionsState::AwaitingDriver;
        Ok(AdminDescribeTransactionsTransition::one(
            AdminDescribeTransactionsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                transactional_id,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        if self.state != AdminDescribeTransactionsState::AwaitingDriver {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        }
        self.state = AdminDescribeTransactionsState::Submitted;
        Ok(AdminDescribeTransactionsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        mut outcome: AdminDescribeTransactionOutcome,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        if self.state != AdminDescribeTransactionsState::Submitted {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        }
        let Some(transactional_id) = self.plan.transactional_ids().get(self.next_transaction)
        else {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        };
        if transactional_id != outcome.transactional_id() {
            return Ok(self.invalid_response());
        }
        let counts = RetainedCounts::from_machine(self);
        let Some(counts) = normalize_outcome(&mut outcome, counts) else {
            return Ok(self.invalid_response());
        };
        self.topic_count = counts.topics;
        self.partition_count = counts.partitions;
        self.topic_bytes = counts.topic_bytes;
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.outcomes.push(outcome);
        self.next_transaction += 1;
        if self.next_transaction == self.plan.transactional_ids().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            let batch =
                AdminDescribeTransactionsBatch::new(self.maximum_throttle_time_ms, outcomes);
            return Ok(self.finish(AdminDescribeTransactionsTerminal::Described(batch)));
        }
        self.submit_current()
    }

    fn finish_awaiting(
        &mut self,
        kind: AdminDescribeTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        if self.state != AdminDescribeTransactionsState::AwaitingDriver {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AdminDescribeTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeTransactionsTransition, AdminDescribeTransactionsMachineError> {
        if self.state != AdminDescribeTransactionsState::Submitted {
            return Err(AdminDescribeTransactionsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    const fn current_unsent_delivery(&self) -> DeliveryStatus {
        if self.next_transaction == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.next_transaction == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    fn invalid_response(&mut self) -> AdminDescribeTransactionsTransition {
        self.finish_failure(
            AdminDescribeTransactionsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        )
    }

    fn finish_failure(
        &mut self,
        kind: AdminDescribeTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminDescribeTransactionsTransition {
        self.finish(AdminDescribeTransactionsTerminal::Failed(
            AdminDescribeTransactionsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AdminDescribeTransactionsTerminal,
    ) -> AdminDescribeTransactionsTransition {
        self.state = AdminDescribeTransactionsState::Completed;
        AdminDescribeTransactionsTransition::one(AdminDescribeTransactionsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
