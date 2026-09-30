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

use self::retry::DirectTopicPartitionCountCall;
pub(super) use self::target::visibility_targets;
use self::{retry::VisibilityRetry, target::CreateTopicVisibilityTarget};
use super::{
    super::DriverOwner,
    create_topics_controller_refresh::{CreateTopicsControllerRefresh, RefreshPoll},
    topic_view::{TopicPartitionCountCall, TopicPartitionCountFailure},
};

pub(super) struct CreateTopicsVisibility {
    targets: Vec<CreateTopicVisibilityTarget>,
    current: usize,
    deadline: OperationDeadline,
    call: Option<VisibilityCall>,
    retry: Option<VisibilityRetry>,
    attempts: usize,
}

enum VisibilityCall {
    Causal(TopicPartitionCountCall),
    Direct(DirectTopicPartitionCountCall),
}

impl VisibilityCall {
    fn try_terminal(&mut self) -> Option<Result<u32, TopicPartitionCountFailure>> {
        match self {
            Self::Causal(call) => call
                .try_terminal()
                .map(|result| result.map(|fact| fact.logical_partition_count)),
            Self::Direct(call) => call.try_terminal(),
        }
    }
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
    controller_refresh: Option<CreateTopicsControllerRefresh>,
}

impl SettledCreateTopicsCall {
    pub(super) fn new(
        operation_id: OperationId,
        input: CreateTopicsInput,
        route_token: Option<RouteFailureToken>,
        visibility_targets: Vec<CreateTopicVisibilityTarget>,
        controller_refresh: Option<CreateTopicsControllerRefresh>,
    ) -> Self {
        Self {
            operation_id,
            input: Some(input),
            route_token,
            visibility_targets: Some(visibility_targets),
            visibility: None,
            controller_refresh,
        }
    }

    pub(crate) const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub(crate) fn take_input(&mut self) -> Option<CreateTopicsInput> {
        if self.controller_refresh.is_some() {
            return None;
        }
        self.input.take()
    }

    pub(super) fn poll_controller_refresh(&mut self, driver: &DriverOwner, now: Moment) -> bool {
        let Some(refresh) = self.controller_refresh.as_mut() else {
            return false;
        };
        match refresh.poll(driver, now) {
            RefreshPoll::Pending => return false,
            RefreshPoll::RetryReady => {
                self.input = Some(CreateTopicsInput::ControllerRouteUnavailable { now });
            }
            RefreshPoll::Failed => {}
        }
        self.controller_refresh = None;
        true
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
        self.input.is_some() && self.controller_refresh.is_none()
    }

    pub(super) fn next_deadline(&self) -> Option<Deadline> {
        self.controller_refresh
            .as_ref()
            .map(CreateTopicsControllerRefresh::next_deadline)
            .or_else(|| {
                self.visibility
                    .as_ref()
                    .map(CreateTopicsVisibility::next_deadline)
            })
    }

    pub(super) fn discard(self) {
        drop(self.route_token);
        drop(self.visibility);
    }

    #[cfg(test)]
    pub(super) fn from_input_for_test(operation_id: OperationId, input: CreateTopicsInput) -> Self {
        Self::new(operation_id, input, None, Vec::new(), None)
    }
}
