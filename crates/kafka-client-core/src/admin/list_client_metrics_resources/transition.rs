//! Atomic API-74 transitions, bounded normalization, and sole terminal assignment.

use core::mem::size_of;

use crate::DeliveryStatus;

use super::{
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES,
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES, LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES,
    ListClientMetricsResourcesEffect, ListClientMetricsResourcesFailure,
    ListClientMetricsResourcesFailureKind, ListClientMetricsResourcesInput,
    ListClientMetricsResourcesListing, ListClientMetricsResourcesMachine,
    ListClientMetricsResourcesMachineError, ListClientMetricsResourcesState,
    ListClientMetricsResourcesTerminal, ListClientMetricsResourcesTransition,
};

impl ListClientMetricsResourcesMachine {
    /// Applies one normalized fact without hidden I/O, retry, cache, or pagination.
    pub fn apply(
        &mut self,
        input: ListClientMetricsResourcesInput,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state == ListClientMetricsResourcesState::Completed {
            return Err(ListClientMetricsResourcesMachineError::AlreadyCompleted);
        }
        match input {
            ListClientMetricsResourcesInput::Start { now } => self.start(now),
            ListClientMetricsResourcesInput::DriverAccepted => self.driver_accepted(),
            ListClientMetricsResourcesInput::DriverRejected => self.finish_awaiting(
                ListClientMetricsResourcesFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            ListClientMetricsResourcesInput::DeadlineElapsed => self.finish_awaiting(
                ListClientMetricsResourcesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            ListClientMetricsResourcesInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    ListClientMetricsResourcesFailureKind::DeadlineElapsed,
                    delivery,
                ),
            ListClientMetricsResourcesInput::BrokerResponded {
                throttle_time_ms,
                resource_names,
            } => self.broker_responded(throttle_time_ms, resource_names),
            ListClientMetricsResourcesInput::BrokerRejected { error } => self
                .finish_submitted_terminal(ListClientMetricsResourcesTerminal::BrokerRejected(
                    error,
                )),
            ListClientMetricsResourcesInput::ResponseTooLarge => self.finish_submitted(
                ListClientMetricsResourcesFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            ListClientMetricsResourcesInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    ListClientMetricsResourcesFailureKind::Compatibility,
                    delivery,
                ),
            ListClientMetricsResourcesInput::TransportFailed { delivery } => {
                self.finish_submitted(ListClientMetricsResourcesFailureKind::Transport, delivery)
            }
            ListClientMetricsResourcesInput::InvalidResponse => self.finish_submitted(
                ListClientMetricsResourcesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state != ListClientMetricsResourcesState::Ready {
            return Err(ListClientMetricsResourcesMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                ListClientMetricsResourcesFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = ListClientMetricsResourcesState::AwaitingDriver;
        Ok(ListClientMetricsResourcesTransition::one(
            ListClientMetricsResourcesEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state != ListClientMetricsResourcesState::AwaitingDriver {
            return Err(ListClientMetricsResourcesMachineError::InvalidState);
        }
        self.state = ListClientMetricsResourcesState::Submitted;
        Ok(ListClientMetricsResourcesTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        mut resource_names: Vec<String>,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state != ListClientMetricsResourcesState::Submitted {
            return Err(ListClientMetricsResourcesMachineError::InvalidState);
        }
        match normalize_resource_names(&mut resource_names) {
            ResourceNamesValidation::Valid => {
                let listing =
                    ListClientMetricsResourcesListing::new(throttle_time_ms, resource_names);
                Ok(self.finish(ListClientMetricsResourcesTerminal::Listed(listing)))
            }
            ResourceNamesValidation::TooLarge => Ok(self.finish_failure(
                ListClientMetricsResourcesFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResourceNamesValidation::Invalid => Ok(self.finish_failure(
                ListClientMetricsResourcesFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    fn finish_awaiting(
        &mut self,
        kind: ListClientMetricsResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state != ListClientMetricsResourcesState::AwaitingDriver {
            return Err(ListClientMetricsResourcesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: ListClientMetricsResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state != ListClientMetricsResourcesState::Submitted {
            return Err(ListClientMetricsResourcesMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: ListClientMetricsResourcesTerminal,
    ) -> Result<ListClientMetricsResourcesTransition, ListClientMetricsResourcesMachineError> {
        if self.state != ListClientMetricsResourcesState::Submitted {
            return Err(ListClientMetricsResourcesMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: ListClientMetricsResourcesFailureKind,
        delivery: DeliveryStatus,
    ) -> ListClientMetricsResourcesTransition {
        self.finish(ListClientMetricsResourcesTerminal::Failed(
            ListClientMetricsResourcesFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: ListClientMetricsResourcesTerminal,
    ) -> ListClientMetricsResourcesTransition {
        self.state = ListClientMetricsResourcesState::Completed;
        ListClientMetricsResourcesTransition::one(ListClientMetricsResourcesEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResourceNamesValidation {
    Valid,
    TooLarge,
    Invalid,
}

fn normalize_resource_names(resource_names: &mut [String]) -> ResourceNamesValidation {
    if resource_names.len() > LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES {
        return ResourceNamesValidation::TooLarge;
    }
    let Some(mut retained_bytes) = resource_names
        .len()
        .checked_mul(size_of::<String>())
        .and_then(|bytes| bytes.checked_add(size_of::<ListClientMetricsResourcesListing>()))
    else {
        return ResourceNamesValidation::TooLarge;
    };
    for name in resource_names.iter() {
        if name.is_empty() {
            return ResourceNamesValidation::Invalid;
        }
        if name.len() > LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES {
            return ResourceNamesValidation::TooLarge;
        }
        let Some(total) = retained_bytes.checked_add(name.len()) else {
            return ResourceNamesValidation::TooLarge;
        };
        retained_bytes = total;
        if retained_bytes > LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES {
            return ResourceNamesValidation::TooLarge;
        }
    }
    resource_names.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if resource_names.windows(2).any(|pair| pair[0] == pair[1]) {
        return ResourceNamesValidation::Invalid;
    }
    ResourceNamesValidation::Valid
}
