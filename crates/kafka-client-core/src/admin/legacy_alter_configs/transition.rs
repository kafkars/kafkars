//! Atomic legacy full-snapshot configuration transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    LegacyAlterConfigsBatch, LegacyAlterConfigsEffect, LegacyAlterConfigsFailure,
    LegacyAlterConfigsFailureKind, LegacyAlterConfigsInput, LegacyAlterConfigsMachine,
    LegacyAlterConfigsMachineError, LegacyAlterConfigsState, LegacyAlterConfigsTerminal,
    LegacyAlterConfigsTransition,
};

impl LegacyAlterConfigsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or fallback.
    pub fn apply(
        &mut self,
        input: LegacyAlterConfigsInput,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state == LegacyAlterConfigsState::Completed {
            return Err(LegacyAlterConfigsMachineError::AlreadyCompleted);
        }
        match input {
            LegacyAlterConfigsInput::Start { now } => self.start(now),
            LegacyAlterConfigsInput::DriverAccepted => self.driver_accepted(),
            LegacyAlterConfigsInput::DriverRejected => self.finish_awaiting(
                LegacyAlterConfigsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            LegacyAlterConfigsInput::DeadlineElapsed => self.finish_awaiting(
                LegacyAlterConfigsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            LegacyAlterConfigsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted_failure(LegacyAlterConfigsFailureKind::DeadlineElapsed, delivery),
            LegacyAlterConfigsInput::BrokerResponded { batch } => self.broker_responded(batch),
            LegacyAlterConfigsInput::ResponseTooLarge => self.finish_submitted_failure(
                LegacyAlterConfigsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            LegacyAlterConfigsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted_failure(LegacyAlterConfigsFailureKind::Compatibility, delivery),
            LegacyAlterConfigsInput::TransportFailed { delivery } => {
                self.finish_submitted_failure(LegacyAlterConfigsFailureKind::Transport, delivery)
            }
            LegacyAlterConfigsInput::InvalidResponse => self.finish_submitted_failure(
                LegacyAlterConfigsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::Ready {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
                LegacyAlterConfigsFailure::new(
                    LegacyAlterConfigsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.routes = self.plan.route_order();
        self.outcomes = vec![None; self.plan.resources().len()];
        Ok(self.submit_current_route())
    }

    fn submit_current_route(&mut self) -> LegacyAlterConfigsTransition {
        let route = self.routes[self.current_route];
        self.state = LegacyAlterConfigsState::AwaitingDriver;
        LegacyAlterConfigsTransition::one(LegacyAlterConfigsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            route,
            plan: self.plan.subplan(route),
        })
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::AwaitingDriver {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        self.state = LegacyAlterConfigsState::Submitted;
        Ok(LegacyAlterConfigsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: LegacyAlterConfigsBatch,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::Submitted {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        if !self.batch_is_correlated(&batch) {
            return Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
                LegacyAlterConfigsFailure::new(
                    LegacyAlterConfigsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        }
        self.merge_batch(batch);
        self.current_route = self.current_route.saturating_add(1);
        if self.current_route < self.routes.len() {
            return Ok(self.submit_current_route());
        }
        let Some(resources) = std::mem::take(&mut self.outcomes)
            .into_iter()
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
                LegacyAlterConfigsFailure::new(
                    LegacyAlterConfigsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        };
        Ok(self.finish(LegacyAlterConfigsTerminal::Configs(
            LegacyAlterConfigsBatch::new(self.throttle_time_ms, resources),
        )))
    }

    fn finish_awaiting(
        &mut self,
        kind: LegacyAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::AwaitingDriver {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        let delivery = self.aggregate_delivery(delivery);
        Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
            LegacyAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted_failure(
        &mut self,
        kind: LegacyAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<LegacyAlterConfigsTransition, LegacyAlterConfigsMachineError> {
        if self.state != LegacyAlterConfigsState::Submitted {
            return Err(LegacyAlterConfigsMachineError::InvalidState);
        }
        let delivery = self.aggregate_delivery(delivery);
        Ok(self.finish(LegacyAlterConfigsTerminal::Failed(
            LegacyAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn aggregate_delivery(&self, route_delivery: DeliveryStatus) -> DeliveryStatus {
        if self.current_route > 0 && route_delivery == DeliveryStatus::NotSent {
            DeliveryStatus::PossiblySent
        } else {
            route_delivery
        }
    }

    fn batch_is_correlated(&self, batch: &LegacyAlterConfigsBatch) -> bool {
        let route = self.routes[self.current_route];
        let route_resource_count = self
            .plan
            .resources()
            .iter()
            .filter(|resource| resource.route() == route)
            .count();
        if batch.resources().len() != route_resource_count {
            return false;
        }
        !self
            .plan
            .resources()
            .iter()
            .filter(|resource| resource.route() == route)
            .zip(batch.resources())
            .any(|(resource, outcome)| {
                resource.resource_type() != outcome.resource_type()
                    || resource.resource_name() != outcome.resource_name()
            })
    }

    fn merge_batch(&mut self, batch: LegacyAlterConfigsBatch) {
        let (throttle_time_ms, resources) = batch.into_parts();
        self.throttle_time_ms = self.throttle_time_ms.max(throttle_time_ms);
        let route = self.routes[self.current_route];
        for ((position, _), outcome) in self
            .plan
            .resources()
            .iter()
            .enumerate()
            .filter(|(_, resource)| resource.route() == route)
            .zip(resources)
        {
            self.outcomes[position] = Some(outcome);
        }
    }

    fn finish(&mut self, terminal: LegacyAlterConfigsTerminal) -> LegacyAlterConfigsTransition {
        self.state = LegacyAlterConfigsState::Completed;
        LegacyAlterConfigsTransition::one(LegacyAlterConfigsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
