//! Caller-ordered partial-result settlement for operation failures.

use crate::DeliveryStatus;

use super::{
    AdminConsumerGroupDescriptionOutcome, AdminDescribeConsumerGroupsBatch,
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsFailure,
    AdminDescribeConsumerGroupsFailureKind, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsMachineError, AdminDescribeConsumerGroupsState,
    AdminDescribeConsumerGroupsTerminal, AdminDescribeConsumerGroupsTransition,
};

impl AdminDescribeConsumerGroupsMachine {
    pub(super) fn finish_awaiting(
        &mut self,
        kind: AdminDescribeConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state != AdminDescribeConsumerGroupsState::AwaitingDriver {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) fn finish_submitted(
        &mut self,
        kind: AdminDescribeConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state != AdminDescribeConsumerGroupsState::Submitted {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    pub(super) const fn current_unsent_delivery(&self) -> DeliveryStatus {
        self.prior_delivery
    }

    pub(super) const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if matches!(self.prior_delivery, DeliveryStatus::NotSent) {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    pub(super) fn finish_failure(
        &mut self,
        kind: AdminDescribeConsumerGroupsFailureKind,
        delivery: DeliveryStatus,
    ) -> AdminDescribeConsumerGroupsTransition {
        let current = self.plan.groups()[self.next_group].clone();
        self.outcomes
            .push(AdminConsumerGroupDescriptionOutcome::operation_failed(
                current,
                AdminDescribeConsumerGroupsFailure::new(kind, delivery),
            ));
        for group_id in &self.plan.groups()[self.next_group + 1..] {
            self.outcomes
                .push(AdminConsumerGroupDescriptionOutcome::operation_failed(
                    group_id.clone(),
                    AdminDescribeConsumerGroupsFailure::new(
                        AdminDescribeConsumerGroupsFailureKind::NotAttempted,
                        DeliveryStatus::NotSent,
                    ),
                ));
        }
        let outcomes = core::mem::take(&mut self.outcomes);
        self.finish(AdminDescribeConsumerGroupsTerminal::Described(
            AdminDescribeConsumerGroupsBatch::new(self.maximum_throttle_time_ms, outcomes),
        ))
    }

    pub(super) fn finish(
        &mut self,
        terminal: AdminDescribeConsumerGroupsTerminal,
    ) -> AdminDescribeConsumerGroupsTransition {
        self.state = AdminDescribeConsumerGroupsState::Completed;
        AdminDescribeConsumerGroupsTransition::one(AdminDescribeConsumerGroupsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
