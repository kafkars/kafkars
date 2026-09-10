//! Causal created-topic visibility under the originating public deadline.

mod drive;
#[cfg(test)]
mod loopback_test;
mod retry;
#[cfg(test)]
mod retry_test;
mod target;

use kafka_client_core::{CreateTopicsInput, Deadline, Moment, OperationId};
use kafka_driver::RouteFailureToken;

use crate::clock::OperationDeadline;

pub(super) use self::target::visibility_targets;
use self::{retry::VisibilityRetry, target::CreateTopicVisibilityTarget};
use super::{super::DriverOwner, topic_view::TopicPartitionCountCall};

pub(super) struct CreateTopicsVisibility {
    targets: Vec<CreateTopicVisibilityTarget>,
    current: usize,
    deadline: OperationDeadline,
    causal_floor: Option<u64>,
    call: Option<TopicPartitionCountCall>,
    retry: Option<VisibilityRetry>,
    attempts: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateTopicsVisibilityPoll {
    Pending,
    Progressed,
    Confirmed,
    Failed,
}

pub(crate) struct SettledCreateTopicsCall {
    operation_id: OperationId,
    input: Option<CreateTopicsInput>,
    route_token: Option<RouteFailureToken>,
    visibility_targets: Option<Vec<CreateTopicVisibilityTarget>>,
    visibility: Option<CreateTopicsVisibility>,
}

impl SettledCreateTopicsCall {
    pub(super) fn new(
        operation_id: OperationId,
        input: CreateTopicsInput,
        route_token: Option<RouteFailureToken>,
        visibility_targets: Vec<CreateTopicVisibilityTarget>,
    ) -> Self {
        Self {
            operation_id,
            input: Some(input),
            route_token,
            visibility_targets: Some(visibility_targets),
            visibility: None,
        }
    }

    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<CreateTopicsInput> {
        self.input.take()
    }

    pub(crate) fn begin_visibility(
        &mut self,
        driver: &DriverOwner,
        operation_id: OperationId,
        deadline: OperationDeadline,
        now: Moment,
    ) -> bool {
        if operation_id != self.operation_id {
            return false;
        }
        let Some(targets) = self.visibility_targets.take() else {
            return false;
        };
        match CreateTopicsVisibility::start(driver, self.route_token.take(), deadline, targets, now)
        {
            Ok(visibility) => {
                self.visibility = Some(visibility);
                true
            }
            Err(()) => false,
        }
    }

    pub(super) fn poll_visibility(&mut self, driver: &DriverOwner, now: Moment) -> bool {
        let Some(visibility) = self.visibility.as_mut() else {
            return false;
        };
        self.input = match visibility.poll(driver, now) {
            CreateTopicsVisibilityPoll::Pending => return false,
            CreateTopicsVisibilityPoll::Progressed => return true,
            CreateTopicsVisibilityPoll::Confirmed => Some(CreateTopicsInput::VisibilityConfirmed),
            CreateTopicsVisibilityPoll::Failed => Some(CreateTopicsInput::VisibilityFailed),
        };
        self.visibility = None;
        true
    }

    pub(super) const fn input_ready(&self) -> bool {
        self.input.is_some()
    }

    pub(super) fn next_deadline(&self) -> Option<Deadline> {
        self.visibility
            .as_ref()
            .map(CreateTopicsVisibility::next_deadline)
    }

    pub(super) fn discard(self) {
        drop(self.route_token);
        drop(self.visibility);
    }

    #[cfg(test)]
    pub(super) fn from_input_for_test(operation_id: OperationId, input: CreateTopicsInput) -> Self {
        Self::new(operation_id, input, None, Vec::new())
    }
}
