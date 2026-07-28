//! Atomic consumer-group description transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    AdminDescribeConsumerGroupsBatch, AdminDescribeConsumerGroupsCallKind,
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsFailureKind,
    AdminDescribeConsumerGroupsInput, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsMachineError, AdminDescribeConsumerGroupsState,
    AdminDescribeConsumerGroupsTerminal, AdminDescribeConsumerGroupsTransition,
};

impl AdminDescribeConsumerGroupsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminDescribeConsumerGroupsInput,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state == AdminDescribeConsumerGroupsState::Completed {
            return Err(AdminDescribeConsumerGroupsMachineError::AlreadyCompleted);
        }
        match input {
            AdminDescribeConsumerGroupsInput::Start { now } => self.start(now),
            AdminDescribeConsumerGroupsInput::DriverAccepted => self.driver_accepted(),
            AdminDescribeConsumerGroupsInput::DriverRejected => self.finish_awaiting(
                AdminDescribeConsumerGroupsFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            AdminDescribeConsumerGroupsInput::DeadlineElapsed => self.finish_awaiting(
                AdminDescribeConsumerGroupsFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            AdminDescribeConsumerGroupsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AdminDescribeConsumerGroupsFailureKind::DeadlineElapsed,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeConsumerGroupsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminDescribeConsumerGroupsInput::FallbackToClassic {
                throttle_time_ms,
                delivery,
            } => self.fallback_to_classic(throttle_time_ms, delivery),
            AdminDescribeConsumerGroupsInput::ResponseTooLarge => self.finish_submitted(
                AdminDescribeConsumerGroupsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminDescribeConsumerGroupsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AdminDescribeConsumerGroupsFailureKind::Compatibility,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeConsumerGroupsInput::TransportFailed { delivery } => self
                .finish_submitted(
                    AdminDescribeConsumerGroupsFailureKind::Transport,
                    self.aggregate_delivery(delivery),
                ),
            AdminDescribeConsumerGroupsInput::InvalidResponse => self.finish_submitted(
                AdminDescribeConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state != AdminDescribeConsumerGroupsState::Ready {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminDescribeConsumerGroupsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        let Some(group_id) = self.plan.groups().get(self.next_group).cloned() else {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        };
        self.state = AdminDescribeConsumerGroupsState::AwaitingDriver;
        Ok(AdminDescribeConsumerGroupsTransition::one(
            AdminDescribeConsumerGroupsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                group_id,
                include_authorized_operations: self.plan.include_authorized_operations(),
                call_kind: self.call_kind,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state != AdminDescribeConsumerGroupsState::AwaitingDriver {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        }
        self.state = AdminDescribeConsumerGroupsState::Submitted;
        Ok(AdminDescribeConsumerGroupsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: super::AdminConsumerGroupDescriptionOutcome,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state != AdminDescribeConsumerGroupsState::Submitted {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        }
        if self.current_group() != Some(outcome.group_id()) {
            return Ok(self.finish_failure(
                AdminDescribeConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.outcomes.push(outcome);
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_group += 1;
        if self.next_group == self.plan.groups().len() {
            let outcomes = core::mem::take(&mut self.outcomes);
            return Ok(self.finish(AdminDescribeConsumerGroupsTerminal::Described(
                AdminDescribeConsumerGroupsBatch::new(self.maximum_throttle_time_ms, outcomes),
            )));
        }
        self.call_kind = AdminDescribeConsumerGroupsCallKind::Consumer;
        self.prior_delivery = DeliveryStatus::NotSent;
        self.submit_current()
    }

    fn fallback_to_classic(
        &mut self,
        throttle_time_ms: u32,
        delivery: DeliveryStatus,
    ) -> Result<AdminDescribeConsumerGroupsTransition, AdminDescribeConsumerGroupsMachineError>
    {
        if self.state != AdminDescribeConsumerGroupsState::Submitted
            || self.call_kind != AdminDescribeConsumerGroupsCallKind::Consumer
        {
            return Err(AdminDescribeConsumerGroupsMachineError::InvalidState);
        }
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.prior_delivery = combined_delivery(self.prior_delivery, delivery);
        self.call_kind = AdminDescribeConsumerGroupsCallKind::ClassicFallback;
        self.submit_current()
    }
}

const fn combined_delivery(left: DeliveryStatus, right: DeliveryStatus) -> DeliveryStatus {
    match (left, right) {
        (DeliveryStatus::NotSent, DeliveryStatus::NotSent) => DeliveryStatus::NotSent,
        _ => DeliveryStatus::PossiblySent,
    }
}
