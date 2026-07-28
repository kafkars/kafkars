//! Terminal-state validation and cumulative delivery classification.

use crate::DeliveryStatus;

use super::super::{
    AdminListConsumerGroupsEffect, AdminListConsumerGroupsFailure,
    AdminListConsumerGroupsFailureKind, AdminListConsumerGroupsMachine,
    AdminListConsumerGroupsMachineError, AdminListConsumerGroupsState,
    AdminListConsumerGroupsTerminal, AdminListConsumerGroupsTransition,
};

impl AdminListConsumerGroupsMachine {
    pub(super) fn finish_awaiting(
        &mut self,
        kind: AdminListConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if !matches!(
            self.state,
            AdminListConsumerGroupsState::AwaitingDiscoveryDriver
                | AdminListConsumerGroupsState::AwaitingBrokerDriver
        ) {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) fn finish_submitted(
        &mut self,
        kind: AdminListConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if !matches!(
            self.state,
            AdminListConsumerGroupsState::DiscoverySubmitted
                | AdminListConsumerGroupsState::BrokerSubmitted
        ) {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
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

    pub(super) fn finish_failure(
        &mut self,
        kind: AdminListConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminListConsumerGroupsTransition {
        self.finish(AdminListConsumerGroupsTerminal::Failed(
            AdminListConsumerGroupsFailure::new(kind, delivery),
        ))
    }

    pub(super) fn finish(
        &mut self,
        terminal: AdminListConsumerGroupsTerminal,
    ) -> AdminListConsumerGroupsTransition {
        self.state = AdminListConsumerGroupsState::Completed;
        AdminListConsumerGroupsTransition::one(AdminListConsumerGroupsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
