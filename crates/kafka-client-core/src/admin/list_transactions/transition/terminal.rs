//! Terminal-state validation and cumulative delivery classification.

use crate::DeliveryStatus;

use super::super::{
    AdminListTransactionsEffect, AdminListTransactionsFailure, AdminListTransactionsFailureKind,
    AdminListTransactionsMachine, AdminListTransactionsMachineError, AdminListTransactionsState,
    AdminListTransactionsTerminal, AdminListTransactionsTransition,
};

impl AdminListTransactionsMachine {
    pub(super) fn finish_awaiting(
        &mut self,
        kind: AdminListTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if !matches!(
            self.state,
            AdminListTransactionsState::AwaitingDiscoveryDriver
                | AdminListTransactionsState::AwaitingBrokerDriver
        ) {
            return Err(AdminListTransactionsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) fn finish_submitted(
        &mut self,
        kind: AdminListTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminListTransactionsTransition, AdminListTransactionsMachineError> {
        if !matches!(
            self.state,
            AdminListTransactionsState::DiscoverySubmitted
                | AdminListTransactionsState::BrokerSubmitted
        ) {
            return Err(AdminListTransactionsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) const fn unsent_delivery(&self) -> DeliveryStatus {
        if self.completed_calls == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    pub(super) const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.completed_calls == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    pub(super) fn invalid_response(&mut self) -> AdminListTransactionsTransition {
        self.finish_failure(
            AdminListTransactionsFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        )
    }

    pub(super) fn finish_failure(
        &mut self,
        kind: AdminListTransactionsFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminListTransactionsTransition {
        self.finish(AdminListTransactionsTerminal::Failed(
            AdminListTransactionsFailure::new(kind, delivery),
        ))
    }

    pub(super) fn finish(
        &mut self,
        terminal: AdminListTransactionsTerminal,
    ) -> AdminListTransactionsTransition {
        self.state = AdminListTransactionsState::Completed;
        AdminListTransactionsTransition::one(AdminListTransactionsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
