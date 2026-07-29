//! Atomic `DescribeConfigs` lifecycle transitions and terminal assignment.

use crate::DeliveryStatus;

use super::{
    DescribeConfigEntry, DescribeConfigResult, DescribeConfigsBatch, DescribeConfigsEffect,
    DescribeConfigsFailure, DescribeConfigsFailureKind, DescribeConfigsInput,
    DescribeConfigsMachine, DescribeConfigsMachineError, DescribeConfigsResourceQuery,
    DescribeConfigsState, DescribeConfigsTerminal, DescribeConfigsTransition,
};

impl DescribeConfigsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or throttle policy.
    pub fn apply(
        &mut self,
        input: DescribeConfigsInput,
    ) -> Result<DescribeConfigsTransition, DescribeConfigsMachineError> {
        if self.state == DescribeConfigsState::Completed {
            return Err(DescribeConfigsMachineError::AlreadyCompleted);
        }
        match input {
            DescribeConfigsInput::Start { now } => self.start(now),
            DescribeConfigsInput::DriverAccepted => self.driver_accepted(),
            DescribeConfigsInput::DriverRejected => self.finish_awaiting(
                DescribeConfigsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeConfigsInput::DeadlineElapsed => self.finish_awaiting(
                DescribeConfigsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeConfigsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted_failure(DescribeConfigsFailureKind::DeadlineElapsed, delivery)
            }
            DescribeConfigsInput::BrokerResponded { batch } => self.broker_responded(batch),
            DescribeConfigsInput::ResponseTooLarge => self.finish_submitted_failure(
                DescribeConfigsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeConfigsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted_failure(DescribeConfigsFailureKind::Compatibility, delivery)
            }
            DescribeConfigsInput::TransportFailed { delivery } => {
                self.finish_submitted_failure(DescribeConfigsFailureKind::Transport, delivery)
            }
            DescribeConfigsInput::InvalidResponse => self.finish_submitted_failure(
                DescribeConfigsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeConfigsTransition, DescribeConfigsMachineError> {
        if self.state != DescribeConfigsState::Ready {
            return Err(DescribeConfigsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish(DescribeConfigsTerminal::Failed(
                DescribeConfigsFailure::new(
                    DescribeConfigsFailureKind::DeadlineElapsed,
                    DeliveryStatus::NotSent,
                ),
            )));
        }
        Ok(self.submit_current_route())
    }

    fn submit_current_route(&mut self) -> DescribeConfigsTransition {
        let route = self.routes[self.current_route];
        self.state = DescribeConfigsState::AwaitingDriver;
        DescribeConfigsTransition::one(DescribeConfigsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            route,
            plan: self.plan.subplan(route),
        })
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeConfigsTransition, DescribeConfigsMachineError> {
        if self.state != DescribeConfigsState::AwaitingDriver {
            return Err(DescribeConfigsMachineError::InvalidState);
        }
        self.state = DescribeConfigsState::Submitted;
        Ok(DescribeConfigsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: DescribeConfigsBatch,
    ) -> Result<DescribeConfigsTransition, DescribeConfigsMachineError> {
        if self.state != DescribeConfigsState::Submitted {
            return Err(DescribeConfigsMachineError::InvalidState);
        }
        self.validate_batch(&batch)?;
        self.merge_batch(batch);
        self.current_route = self.current_route.saturating_add(1);
        if self.current_route < self.routes.len() {
            return Ok(self.submit_current_route());
        }
        let resources = std::mem::take(&mut self.outcomes)
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(DescribeConfigsMachineError::OutcomeCountMismatch)?;
        Ok(
            self.finish(DescribeConfigsTerminal::Configs(DescribeConfigsBatch::new(
                self.throttle_time_ms,
                resources,
            ))),
        )
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeConfigsTransition, DescribeConfigsMachineError> {
        if self.state != DescribeConfigsState::AwaitingDriver {
            return Err(DescribeConfigsMachineError::InvalidState);
        }
        let delivery = self.aggregate_delivery(delivery);
        Ok(self.finish(DescribeConfigsTerminal::Failed(
            DescribeConfigsFailure::new(kind, delivery),
        )))
    }

    fn finish_submitted_failure(
        &mut self,
        kind: DescribeConfigsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeConfigsTransition, DescribeConfigsMachineError> {
        if self.state != DescribeConfigsState::Submitted {
            return Err(DescribeConfigsMachineError::InvalidState);
        }
        let delivery = self.aggregate_delivery(delivery);
        Ok(self.finish(DescribeConfigsTerminal::Failed(
            DescribeConfigsFailure::new(kind, delivery),
        )))
    }

    fn aggregate_delivery(&self, route_delivery: DeliveryStatus) -> DeliveryStatus {
        if self.current_route > 0 && route_delivery == DeliveryStatus::NotSent {
            DeliveryStatus::PossiblySent
        } else {
            route_delivery
        }
    }

    fn validate_batch(
        &self,
        batch: &DescribeConfigsBatch,
    ) -> Result<(), DescribeConfigsMachineError> {
        let route = self.routes[self.current_route];
        let route_resource_count = self
            .plan
            .resources()
            .iter()
            .filter(|query| query.route() == route)
            .count();
        if batch.resources().len() != route_resource_count {
            return Err(DescribeConfigsMachineError::OutcomeCountMismatch);
        }
        for (query, outcome) in self
            .plan
            .resources()
            .iter()
            .filter(|query| query.route() == route)
            .zip(batch.resources())
        {
            if query.resource_type() != outcome.resource_type()
                || query.resource_name() != outcome.resource_name()
            {
                return Err(DescribeConfigsMachineError::OutcomeResourceMismatch);
            }
            if let DescribeConfigResult::Configs(configs) = outcome.result() {
                validate_configs(query, configs)?;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, batch: DescribeConfigsBatch) {
        let (throttle_time_ms, resources) = batch.into_parts();
        self.throttle_time_ms = self.throttle_time_ms.max(throttle_time_ms);
        let route = self.routes[self.current_route];
        for ((position, _), outcome) in self
            .plan
            .resources()
            .iter()
            .enumerate()
            .filter(|(_, query)| query.route() == route)
            .zip(resources)
        {
            self.outcomes[position] = Some(outcome);
        }
    }

    fn finish(&mut self, terminal: DescribeConfigsTerminal) -> DescribeConfigsTransition {
        self.state = DescribeConfigsState::Completed;
        DescribeConfigsTransition::one(DescribeConfigsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn validate_configs(
    query: &DescribeConfigsResourceQuery,
    configs: &[DescribeConfigEntry],
) -> Result<(), DescribeConfigsMachineError> {
    let Some(keys) = query.configuration_keys() else {
        if configs.iter().any(|config| config.name().is_empty())
            || configs
                .windows(2)
                .any(|pair| pair[0].name() >= pair[1].name())
        {
            return Err(DescribeConfigsMachineError::ConfigurationCorrelationMismatch);
        }
        return Ok(());
    };
    let mut remaining = keys;
    for config in configs {
        let Some(index) = remaining.iter().position(|key| key == config.name()) else {
            return Err(DescribeConfigsMachineError::ConfigurationCorrelationMismatch);
        };
        remaining = &remaining[index.saturating_add(1)..];
    }
    Ok(())
}
