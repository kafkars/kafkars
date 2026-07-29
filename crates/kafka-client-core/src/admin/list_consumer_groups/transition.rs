//! Atomic discovery, exact-broker iteration, merge, and terminal assignment.

mod terminal;

use crate::DeliveryStatus;

use super::{
    AdminConsumerGroupListing, AdminGroupListingScope, AdminListConsumerGroupsBatch,
    AdminListConsumerGroupsEffect, AdminListConsumerGroupsFailureKind,
    AdminListConsumerGroupsInput, AdminListConsumerGroupsMachine,
    AdminListConsumerGroupsMachineError, AdminListConsumerGroupsState,
    AdminListConsumerGroupsTerminal, AdminListConsumerGroupsTransition,
};

impl AdminListConsumerGroupsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AdminListConsumerGroupsInput,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if self.state == AdminListConsumerGroupsState::Completed {
            return Err(AdminListConsumerGroupsMachineError::AlreadyCompleted);
        }
        match input {
            AdminListConsumerGroupsInput::Start { now } => self.start(now),
            AdminListConsumerGroupsInput::DriverAccepted => self.driver_accepted(),
            AdminListConsumerGroupsInput::DriverRejected => self.finish_awaiting(
                AdminListConsumerGroupsFailureKind::DriverRejected,
                self.unsent_delivery(),
            ),
            AdminListConsumerGroupsInput::DeadlineElapsed => self.finish_awaiting(
                AdminListConsumerGroupsFailureKind::DeadlineElapsed,
                self.unsent_delivery(),
            ),
            AdminListConsumerGroupsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AdminListConsumerGroupsFailureKind::DeadlineElapsed,
                    self.aggregate_delivery(delivery),
                ),
            AdminListConsumerGroupsInput::BrokersDiscovered { broker_ids } => {
                self.brokers_discovered(broker_ids)
            }
            AdminListConsumerGroupsInput::DiscoveryRejected { error } => {
                self.discovery_rejected(error)
            }
            AdminListConsumerGroupsInput::BrokerResponded {
                throttle_time_ms,
                outcome,
            } => self.broker_responded(throttle_time_ms, outcome),
            AdminListConsumerGroupsInput::ResponseTooLarge => self.finish_submitted(
                AdminListConsumerGroupsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AdminListConsumerGroupsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AdminListConsumerGroupsFailureKind::Compatibility,
                    self.aggregate_delivery(delivery),
                ),
            AdminListConsumerGroupsInput::TransportFailed { delivery } => self.finish_submitted(
                AdminListConsumerGroupsFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            AdminListConsumerGroupsInput::InvalidResponse => self.finish_submitted(
                AdminListConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if self.state != AdminListConsumerGroupsState::Ready {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AdminListConsumerGroupsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AdminListConsumerGroupsState::AwaitingDiscoveryDriver;
        Ok(AdminListConsumerGroupsTransition::one(
            AdminListConsumerGroupsEffect::SubmitDiscovery {
                operation_id: self.operation_id,
                deadline: self.deadline,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        self.state = match self.state {
            AdminListConsumerGroupsState::AwaitingDiscoveryDriver => {
                AdminListConsumerGroupsState::DiscoverySubmitted
            }
            AdminListConsumerGroupsState::AwaitingBrokerDriver => {
                AdminListConsumerGroupsState::BrokerSubmitted
            }
            _ => return Err(AdminListConsumerGroupsMachineError::InvalidState),
        };
        Ok(AdminListConsumerGroupsTransition::none())
    }

    fn brokers_discovered(
        &mut self,
        mut broker_ids: Vec<i32>,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if self.state != AdminListConsumerGroupsState::DiscoverySubmitted {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
        }
        broker_ids.sort_unstable();
        if broker_ids.is_empty()
            || broker_ids.iter().any(|broker| *broker < 0)
            || broker_ids.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Ok(self.finish_failure(
                AdminListConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.completed_calls = 1;
        self.broker_ids = broker_ids;
        self.submit_current_broker()
    }

    fn discovery_rejected(
        &mut self,
        error: super::super::DescribeClusterBrokerError,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if self.state != AdminListConsumerGroupsState::DiscoverySubmitted {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
        }
        Ok(self.finish(AdminListConsumerGroupsTerminal::DiscoveryRejected(error)))
    }

    fn submit_current_broker(
        &mut self,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        let Some(broker_id) = self.current_broker() else {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
        };
        self.state = AdminListConsumerGroupsState::AwaitingBrokerDriver;
        Ok(AdminListConsumerGroupsTransition::one(
            AdminListConsumerGroupsEffect::SubmitBroker {
                operation_id: self.operation_id,
                deadline: self.deadline,
                broker_id,
                filters: self.filters.clone(),
            },
        ))
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        outcome: super::AdminListConsumerGroupsBrokerOutcome,
    ) -> Result<AdminListConsumerGroupsTransition, AdminListConsumerGroupsMachineError> {
        if self.state != AdminListConsumerGroupsState::BrokerSubmitted {
            return Err(AdminListConsumerGroupsMachineError::InvalidState);
        }
        if self.current_broker() != Some(outcome.broker_id()) {
            return Ok(self.finish_failure(
                AdminListConsumerGroupsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        match outcome {
            super::AdminListConsumerGroupsBrokerOutcome::Groups { groups, .. } => {
                let scope = self.scope;
                self.groups.extend(groups.into_iter().filter(|group| {
                    (scope == AdminGroupListingScope::All || group.is_consumer_group())
                        && self.filters.retains_protocol_type(group.protocol_type())
                }));
            }
            super::AdminListConsumerGroupsBrokerOutcome::Rejected(error) => {
                self.broker_errors.push(error);
            }
        }
        self.maximum_throttle_time_ms = self.maximum_throttle_time_ms.max(throttle_time_ms);
        self.next_broker += 1;
        self.completed_calls += 1;
        if self.next_broker == self.broker_ids.len() {
            self.groups.sort_unstable_by(compare_group);
            self.groups
                .dedup_by(|right, left| right.group_id() == left.group_id());
            let groups = core::mem::take(&mut self.groups);
            let broker_errors = core::mem::take(&mut self.broker_errors);
            return Ok(self.finish(AdminListConsumerGroupsTerminal::Listed(
                AdminListConsumerGroupsBatch::new(
                    self.maximum_throttle_time_ms,
                    groups,
                    broker_errors,
                ),
            )));
        }
        self.submit_current_broker()
    }
}

fn compare_group(
    left: &AdminConsumerGroupListing,
    right: &AdminConsumerGroupListing,
) -> core::cmp::Ordering {
    left.group_id()
        .as_bytes()
        .cmp(right.group_id().as_bytes())
        .then_with(|| {
            left.protocol_type()
                .as_bytes()
                .cmp(right.protocol_type().as_bytes())
        })
        .then_with(|| left.group_state().cmp(&right.group_state()))
        .then_with(|| left.group_type().cmp(&right.group_type()))
}
