//! Atomic incremental configuration transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    IncrementalAlterConfigsBatch, IncrementalAlterConfigsEffect, IncrementalAlterConfigsFailure,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsInput,
    IncrementalAlterConfigsMachine, IncrementalAlterConfigsMachineError,
    IncrementalAlterConfigsState, IncrementalAlterConfigsTerminal,
    IncrementalAlterConfigsTransition,
};

impl IncrementalAlterConfigsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or fallback.
    pub fn apply(
        &mut self,
        input: IncrementalAlterConfigsInput,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state == IncrementalAlterConfigsState::Completed {
            return Err(IncrementalAlterConfigsMachineError::AlreadyCompleted);
        }
        match input {
            IncrementalAlterConfigsInput::Start { now } => self.start(now),
            IncrementalAlterConfigsInput::DriverAccepted => self.driver_accepted(),
            IncrementalAlterConfigsInput::DriverRejected => self.finish_awaiting(
                IncrementalAlterConfigsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            IncrementalAlterConfigsInput::DeadlineElapsed => self.finish_awaiting(
                IncrementalAlterConfigsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            IncrementalAlterConfigsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted_failure(
                    IncrementalAlterConfigsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            IncrementalAlterConfigsInput::BrokerResponded { batch } => self.broker_responded(batch),
            IncrementalAlterConfigsInput::ResponseTooLarge => self.finish_submitted_failure(
                IncrementalAlterConfigsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            IncrementalAlterConfigsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted_failure(
                    IncrementalAlterConfigsFailureKind::Compatibility,
                    delivery,
                ),
            IncrementalAlterConfigsInput::TransportFailed { delivery } => self
                .finish_submitted_failure(IncrementalAlterConfigsFailureKind::Transport, delivery),
            IncrementalAlterConfigsInput::InvalidResponse => self.finish_submitted_failure(
                IncrementalAlterConfigsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::Ready {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
                IncrementalAlterConfigsFailure::new(
                    IncrementalAlterConfigsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        self.routes = self.plan.route_order();
        self.outcomes = vec![None; self.plan.resources().len()];
        Ok(self.submit_current_route())
    }

    fn submit_current_route(&mut self) -> IncrementalAlterConfigsTransition {
        let route = self.routes[self.current_route];
        self.state = IncrementalAlterConfigsState::AwaitingDriver;
        IncrementalAlterConfigsTransition::one(IncrementalAlterConfigsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            route,
            plan: self.plan.subplan(route),
        })
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::AwaitingDriver {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        self.state = IncrementalAlterConfigsState::Submitted;
        Ok(IncrementalAlterConfigsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: IncrementalAlterConfigsBatch,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::Submitted {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        if !self.batch_is_correlated(&batch) {
            return Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
                IncrementalAlterConfigsFailure::new(
                    IncrementalAlterConfigsFailureKind::InvalidResponse,
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
            return Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
                IncrementalAlterConfigsFailure::new(
                    IncrementalAlterConfigsFailureKind::InvalidResponse,
                    DeliveryStatus::PossiblySent,
                ),
            )));
        };
        Ok(self.finish(IncrementalAlterConfigsTerminal::Configs(
            IncrementalAlterConfigsBatch::new(self.throttle_time_ms, resources),
        )))
    }

    fn finish_awaiting(
        &mut self,
        kind: IncrementalAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::AwaitingDriver {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        let delivery = self.aggregate_delivery(delivery);
        Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
            IncrementalAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted_failure(
        &mut self,
        kind: IncrementalAlterConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<IncrementalAlterConfigsTransition, IncrementalAlterConfigsMachineError> {
        if self.state != IncrementalAlterConfigsState::Submitted {
            return Err(IncrementalAlterConfigsMachineError::InvalidState);
        }
        let delivery = self.aggregate_delivery(delivery);
        Ok(self.finish(IncrementalAlterConfigsTerminal::Failed(
            IncrementalAlterConfigsFailure::new(kind, delivery),
        )))
    }

    fn aggregate_delivery(&self, route_delivery: DeliveryStatus) -> DeliveryStatus {
        if self.current_route > 0 && route_delivery == DeliveryStatus::NotSent {
            DeliveryStatus::PossiblySent
        } else {
            route_delivery
        }
    }

    fn batch_is_correlated(&self, batch: &IncrementalAlterConfigsBatch) -> bool {
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

    fn merge_batch(&mut self, batch: IncrementalAlterConfigsBatch) {
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

    fn finish(
        &mut self,
        terminal: IncrementalAlterConfigsTerminal,
    ) -> IncrementalAlterConfigsTransition {
        self.state = IncrementalAlterConfigsState::Completed;
        IncrementalAlterConfigsTransition::one(IncrementalAlterConfigsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
