//! Atomic API-74 v1 transitions, bounded normalization, and terminal assignment.

use core::cmp::Ordering;

use crate::DeliveryStatus;

use super::{
    LIST_CONFIG_RESOURCES_MAX_RESOURCE_NAME_BYTES, LIST_CONFIG_RESOURCES_MAX_RESOURCES,
    LIST_CONFIG_RESOURCES_MAX_TEXT_BYTES, ListConfigResourcesEffect, ListConfigResourcesFailure,
    ListConfigResourcesFailureKind, ListConfigResourcesInput, ListConfigResourcesListing,
    ListConfigResourcesMachine, ListConfigResourcesMachineError, ListConfigResourcesState,
    ListConfigResourcesTerminal, ListConfigResourcesTransition, ListedConfigResource,
};

impl ListConfigResourcesMachine {
    /// Applies one normalized fact without hidden I/O, retry, fallback, or cancellation.
    pub fn apply(
        &mut self,
        input: ListConfigResourcesInput,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state == ListConfigResourcesState::Completed {
            return Err(ListConfigResourcesMachineError::AlreadyCompleted);
        }
        match input {
            ListConfigResourcesInput::Start { now } => self.start(now),
            ListConfigResourcesInput::DriverAccepted => self.driver_accepted(),
            ListConfigResourcesInput::DriverRejected => self.finish_awaiting(
                ListConfigResourcesFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            ListConfigResourcesInput::DeadlineElapsed => self.finish_awaiting(
                ListConfigResourcesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            ListConfigResourcesInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(ListConfigResourcesFailureKind::DeadlineElapsed, delivery)
            }
            ListConfigResourcesInput::BrokerResponded {
                throttle_time_ms,
                resources,
            } => self.broker_responded(throttle_time_ms, resources),
            ListConfigResourcesInput::BrokerRejected { error } => {
                self.finish_submitted_terminal(ListConfigResourcesTerminal::BrokerRejected(error))
            }
            ListConfigResourcesInput::ResponseTooLarge => self.finish_submitted(
                ListConfigResourcesFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ListConfigResourcesInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(ListConfigResourcesFailureKind::Compatibility, delivery)
            }
            ListConfigResourcesInput::TransportFailed { delivery } => {
                self.finish_submitted(ListConfigResourcesFailureKind::Transport, delivery)
            }
            ListConfigResourcesInput::InvalidResponse => self.finish_submitted(
                ListConfigResourcesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state != ListConfigResourcesState::Ready {
            return Err(ListConfigResourcesMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                ListConfigResourcesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = ListConfigResourcesState::AwaitingDriver;
        Ok(ListConfigResourcesTransition::one(
            ListConfigResourcesEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state != ListConfigResourcesState::AwaitingDriver {
            return Err(ListConfigResourcesMachineError::InvalidState);
        }
        self.state = ListConfigResourcesState::Submitted;
        Ok(ListConfigResourcesTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        mut resources: Vec<ListedConfigResource>,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state != ListConfigResourcesState::Submitted {
            return Err(ListConfigResourcesMachineError::InvalidState);
        }
        match normalize_resources(&mut resources) {
            ResourcesValidation::Valid => {
                let listing = ListConfigResourcesListing::new(throttle_time_ms, resources);
                Ok(self.finish(ListConfigResourcesTerminal::Listed(listing)))
            }
            ResourcesValidation::TooLarge => Ok(self.finish_failure(
                ListConfigResourcesFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResourcesValidation::Invalid => Ok(self.finish_failure(
                ListConfigResourcesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    fn finish_awaiting(
        &mut self,
        kind: ListConfigResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state != ListConfigResourcesState::AwaitingDriver {
            return Err(ListConfigResourcesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: ListConfigResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state != ListConfigResourcesState::Submitted {
            return Err(ListConfigResourcesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: ListConfigResourcesTerminal,
    ) -> Result<ListConfigResourcesTransition, ListConfigResourcesMachineError> {
        if self.state != ListConfigResourcesState::Submitted {
            return Err(ListConfigResourcesMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: ListConfigResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> ListConfigResourcesTransition {
        self.finish(ListConfigResourcesTerminal::Failed(
            ListConfigResourcesFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: ListConfigResourcesTerminal) -> ListConfigResourcesTransition {
        self.state = ListConfigResourcesState::Completed;
        ListConfigResourcesTransition::one(ListConfigResourcesEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResourcesValidation {
    Valid,
    TooLarge,
    Invalid,
}

fn normalize_resources(resources: &mut [ListedConfigResource]) -> ResourcesValidation {
    if resources.len() > LIST_CONFIG_RESOURCES_MAX_RESOURCES {
        return ResourcesValidation::TooLarge;
    }
    let mut text_bytes = 0usize;
    for resource in resources.iter() {
        if resource.resource_name().is_empty() {
            return ResourcesValidation::Invalid;
        }
        if resource.resource_name().len() > LIST_CONFIG_RESOURCES_MAX_RESOURCE_NAME_BYTES {
            return ResourcesValidation::TooLarge;
        }
        let Some(total) = text_bytes.checked_add(resource.resource_name().len()) else {
            return ResourcesValidation::TooLarge;
        };
        text_bytes = total;
        if text_bytes > LIST_CONFIG_RESOURCES_MAX_TEXT_BYTES {
            return ResourcesValidation::TooLarge;
        }
    }
    resources.sort_unstable_by(compare_resources);
    if resources.windows(2).any(|pair| {
        pair[0].resource_type() == pair[1].resource_type()
            && pair[0].resource_name() == pair[1].resource_name()
    }) {
        return ResourcesValidation::Invalid;
    }
    ResourcesValidation::Valid
}

fn compare_resources(left: &ListedConfigResource, right: &ListedConfigResource) -> Ordering {
    left.resource_type()
        .code()
        .cmp(&right.resource_type().code())
        .then_with(|| {
            left.resource_name()
                .as_bytes()
                .cmp(right.resource_name().as_bytes())
        })
}
