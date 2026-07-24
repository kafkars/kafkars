//! Exact deadline pairing and local admission for one position lookup.

use kafka_client_core::{AssignedConsumerEffect, Deadline, Moment, PositionFence, StartPosition};
use kafka_driver::RoutedCall;
use kafka_wire::ListOffsetsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::consumer::{ListOffsetsIsolation, list_offsets_request, remaining_timeout_ms},
};

use super::{super::DriverOwner, list_offsets_terminal::PositionResolutionTerminal};

/// Exact core effect facts paired with engine catalog and isolation facts.
#[must_use = "a prepared position lookup must be submitted or terminally settled"]
pub(crate) struct PositionResolutionRequest {
    fence: PositionFence,
    position: StartPosition,
    topic: String,
    isolation: ListOffsetsIsolation,
    operation_deadline: OperationDeadline,
}

impl PositionResolutionRequest {
    pub(crate) fn from_effect(
        effect: AssignedConsumerEffect,
        topic: String,
        isolation: ListOffsetsIsolation,
        operation_deadline: OperationDeadline,
    ) -> Result<Self, PositionRequestPreparationError> {
        let AssignedConsumerEffect::ResolvePosition {
            fence,
            position,
            deadline,
        } = effect
        else {
            return Err(PositionRequestPreparationError::UnexpectedEffect);
        };
        if deadline != operation_deadline.core() {
            return Err(PositionRequestPreparationError::DeadlineMismatch {
                effect: deadline,
                operation: operation_deadline.core(),
            });
        }
        Ok(Self {
            fence,
            position,
            topic,
            isolation,
            operation_deadline,
        })
    }
}

/// Failure to pair one core effect with its exact call-boundary deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PositionRequestPreparationError {
    UnexpectedEffect,
    DeadlineMismatch {
        effect: Deadline,
        operation: Deadline,
    },
}

pub(super) struct AcceptedPositionCall {
    pub(super) fence: PositionFence,
    pub(super) topic: String,
    pub(super) isolation: ListOffsetsIsolation,
    pub(super) call: RoutedCall<ListOffsetsResponse>,
}

pub(super) fn submit_position_request(
    driver: &DriverOwner,
    request: PositionResolutionRequest,
    now: Moment,
) -> Result<AcceptedPositionCall, PositionAdmissionFailure> {
    let timeout_ms = remaining_timeout_ms(now, request.operation_deadline.core())
        .map_err(|_error| PositionAdmissionFailure::new(request.fence, now))?;
    let partition = request.fence.partition().partition();
    let generated = list_offsets_request(
        &request.topic,
        partition,
        request.position,
        request.isolation,
        timeout_ms,
    )
    .map_err(|_error| PositionAdmissionFailure::new(request.fence, now))?;
    let partition_index = i32::try_from(partition.get())
        .map_err(|_| PositionAdmissionFailure::new(request.fence, now))?;
    let call = driver
        .submit_tracked_list_offsets(
            &request.topic,
            partition_index,
            generated,
            request.operation_deadline.transport(),
        )
        .map_err(|_error| PositionAdmissionFailure::new(request.fence, now))?;
    Ok(AcceptedPositionCall {
        fence: request.fence,
        topic: request.topic,
        isolation: request.isolation,
        call,
    })
}

/// Definitely-unsent local failure converted back into the same fenced core input.
#[derive(Debug)]
pub(crate) struct PositionAdmissionFailure {
    terminal: PositionResolutionTerminal,
}

impl PositionAdmissionFailure {
    pub(super) fn new(fence: PositionFence, now: Moment) -> Self {
        Self {
            terminal: PositionResolutionTerminal::failed(fence, now),
        }
    }

    pub(crate) const fn terminal(&self) -> PositionResolutionTerminal {
        self.terminal
    }
}
