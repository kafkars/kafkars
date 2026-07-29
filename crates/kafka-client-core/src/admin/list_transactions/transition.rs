//! Atomic discovery, exact-broker fanout, aggregation, and terminal assignment.

mod terminal;

use crate::DeliveryStatus;

use super::{
    AdminListTransactionsBatch, AdminListTransactionsBrokerOutcome, AdminListTransactionsEffect,
    AdminListTransactionsFailureKind, AdminListTransactionsInput, AdminListTransactionsMachine,
    AdminListTransactionsMachineError, AdminListTransactionsState, AdminListTransactionsTerminal,
    AdminListTransactionsTransition, LIST_TRANSACTIONS_MAX_BROKERS,
    normalization::{RetainedCounts, canonicalize, retain_listing},
};

impl AdminListTransactionsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminListTransactionsInput,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if self.state == AdminListTransactionsState::Completed {
            return Err(AdminListTransactionsMachineError::AlreadyCompleted);
        }
        match input {
            AdminListTransactionsInput::Start { now } => self.start(now),
            AdminListTransactionsInput::DriverAccepted => self.driver_accepted(),
            AdminListTransactionsInput::DriverRejected => self.finish_awaiting(
                AdminListTransactionsFailureKind::DriverRejected,
                self.unsent_delivery(),
            ),
            AdminListTransactionsInput::DeadlineElapsed => self.finish_awaiting(
                AdminListTransactionsFailureKind::DeadlineElapsed,
                self.unsent_delivery(),
            ),
            AdminListTransactionsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AdminListTransactionsFailureKind::DeadlineElapsed,
                    self.aggregate_delivery(delivery),
                ),
            AdminListTransactionsInput::BrokersDiscovered { broker_ids } => {
                self.brokers_discovered(broker_ids)
            }
            AdminListTransactionsInput::DiscoveryRejected { error } => {
                self.discovery_rejected(error)
            }
            AdminListTransactionsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminListTransactionsInput::ResponseTooLarge => self.finish_submitted(
                AdminListTransactionsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminListTransactionsInput::ProtocolIncompatible { delivery } => self.finish_submitted(
                AdminListTransactionsFailureKind::Compatibility,
                self.aggregate_delivery(delivery),
            ),
            AdminListTransactionsInput::TransportFailed { delivery } => self.finish_submitted(
                AdminListTransactionsFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            AdminListTransactionsInput::InvalidResponse => self.finish_submitted(
                AdminListTransactionsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if self.state != AdminListTransactionsState::Ready {
            return Err(AdminListTransactionsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminListTransactionsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AdminListTransactionsState::AwaitingDiscoveryDriver;
        Ok(AdminListTransactionsTransition::one(
            AdminListTransactionsEffect::SubmitDiscovery {
                operation_id: self.operation_id,
                deadline: self.deadline,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        self.state = match self.state {
            AdminListTransactionsState::AwaitingDiscoveryDriver => {
                AdminListTransactionsState::DiscoverySubmitted
            }
            AdminListTransactionsState::AwaitingBrokerDriver => {
                AdminListTransactionsState::BrokerSubmitted
            }
            _ => return Err(AdminListTransactionsMachineError::InvalidState),
        };
        Ok(AdminListTransactionsTransition::none())
    }

    fn brokers_discovered(
        &mut self,
        mut broker_ids: Vec<i32>,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if self.state != AdminListTransactionsState::DiscoverySubmitted {
            return Err(AdminListTransactionsMachineError::InvalidState);
        }
        if broker_ids.len() > LIST_TRANSACTIONS_MAX_BROKERS {
            return Ok(self.finish_failure(
                AdminListTransactionsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ));
        }
        broker_ids.sort_unstable();
        if broker_ids.is_empty()
            || broker_ids.iter().any(|broker| *broker < 0)
            || broker_ids.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Ok(self.invalid_response());
        }
        self.completed_calls = 1;
        self.broker_ids = broker_ids;
        self.submit_current_broker()
    }

    fn discovery_rejected(
        &mut self,
        error: super::super::DescribeClusterBrokerError,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if self.state != AdminListTransactionsState::DiscoverySubmitted {
            return Err(AdminListTransactionsMachineError::InvalidState);
        }
        Ok(self.finish(AdminListTransactionsTerminal::DiscoveryRejected(error)))
    }

    fn submit_current_broker(
        &mut self,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        let Some(broker_id) = self.current_broker() else {
            return Err(AdminListTransactionsMachineError::InvalidState);
        };
        self.state = AdminListTransactionsState::AwaitingBrokerDriver;
        Ok(AdminListTransactionsTransition::one(
            AdminListTransactionsEffect::SubmitBroker {
                operation_id: self.operation_id,
                deadline: self.deadline,
                broker_id,
                plan: self.plan.clone(),
            },
        ))
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: AdminListTransactionsBrokerOutcome,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if self.state != AdminListTransactionsState::BrokerSubmitted {
            return Err(AdminListTransactionsMachineError::InvalidState);
        }
        if self.current_broker() != Some(outcome.broker_id()) {
            return Ok(self.invalid_response());
        }
        match outcome {
            AdminListTransactionsBrokerOutcome::Listed {
                unknown_state_filters,
                transactions,
                ..
            } => {
                let retained = RetainedCounts::new(
                    self.unknown_state_filter_count,
                    self.transaction_count,
                    self.result_string_bytes,
                );
                let Some(retained) =
                    retain_listing(&unknown_state_filters, &transactions, retained)
                else {
                    return Ok(self.finish_failure(
                        AdminListTransactionsFailureKind::ResponseTooLarge,
                        DeliveryStatus::PossiblySent,
                    ));
                };
                self.unknown_state_filter_count = retained.unknown_state_filters;
                self.transaction_count = retained.transactions;
                self.result_string_bytes = retained.string_bytes;
                self.unknown_state_filters.extend(unknown_state_filters);
                self.transactions.extend(transactions);
            }
            AdminListTransactionsBrokerOutcome::Rejected(error) => {
                self.broker_errors.push(error);
            }
        }
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_broker += 1;
        self.completed_calls += 1;
        if self.next_broker != self.broker_ids.len() {
            return self.submit_current_broker();
        }
        if !canonicalize(&mut self.unknown_state_filters, &mut self.transactions) {
            return Ok(self.invalid_response());
        }
        let batch = AdminListTransactionsBatch::new(
            self.maximum_throttle_time_ms,
            core::mem::take(&mut self.unknown_state_filters),
            core::mem::take(&mut self.transactions),
            core::mem::take(&mut self.broker_errors),
        );
        Ok(self.finish(AdminListTransactionsTerminal::Listed(batch)))
    }
}
