//! Linear ownership of one exact static-member `LeaveGroup` admin call.

use std::{error::Error, fmt};

use kafka_client_core::RemoveConsumerGroupMembersPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::LeaveGroupResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::remove_consumer_group_members::{
        RemoveConsumerGroupMembersRequestFailure, remove_consumer_group_members_request,
    },
};

use super::{
    super::DriverOwner,
    remove_consumer_group_members_submission::RemoveConsumerGroupMembersSubmitError,
    remove_consumer_group_members_terminal::{
        RecoveredRemoveConsumerGroupMembersCall, RemoveConsumerGroupMembersTerminal,
        retain_remove_consumer_group_members_terminal,
    },
};

/// One accepted driver call retained beside its concrete operation owner.
#[must_use = "an accepted consumer-group member removal must be terminally settled"]
pub(crate) struct RemoveConsumerGroupMembersCall {
    call: Option<RoutedCall<LeaveGroupResponse>>,
    plan: Option<RemoveConsumerGroupMembersPlan>,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl RemoveConsumerGroupMembersCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: OperationDeadline,
    ) -> Result<Self, RemoveConsumerGroupMembersCallAdmissionFailure> {
        let (request, minimum_version) =
            match remove_consumer_group_members_request(&plan, request_scratch_limit) {
                Ok(request) => request,
                Err(source) => {
                    return Err(RemoveConsumerGroupMembersCallAdmissionFailure::request(
                        source,
                        plan,
                        request_scratch_limit,
                        result_limit,
                    ));
                }
            };
        let call = match driver.submit_tracked_remove_consumer_group_members(
            plan.group_id(),
            request,
            minimum_version,
            deadline,
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(RemoveConsumerGroupMembersCallAdmissionFailure::driver(
                    source,
                    plan,
                    request_scratch_limit,
                    result_limit,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
            request_scratch_limit,
            result_limit,
        })
    }

    /// Extracts a ready terminal once without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<RemoveConsumerGroupMembersTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_remove_consumer_group_members_terminal(
                    plan,
                    self.request_scratch_limit,
                    self.result_limit,
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches(
        &self,
        expected_plan: &RemoveConsumerGroupMembersPlan,
        expected_request_scratch_limit: usize,
        expected_result_limit: usize,
    ) -> bool {
        self.plan.as_ref().is_some_and(|plan| {
            plan == expected_plan
                && self.request_scratch_limit == expected_request_scratch_limit
                && self.result_limit == expected_result_limit
        })
    }

    /// Seals an unresolved accepted call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Result<RecoveredRemoveConsumerGroupMembersCall, Self> {
        if self.call.is_none() || self.plan.is_none() {
            return Err(self);
        }
        let Self {
            call,
            plan,
            request_scratch_limit,
            result_limit,
        } = self;
        drop(call);
        Ok(RecoveredRemoveConsumerGroupMembersCall::new(
            plan.unwrap_or_else(|| unreachable!("validated exact plan")),
            request_scratch_limit,
            result_limit,
        ))
    }
}

/// Definitely-unsent request-construction or driver-admission failure.
#[must_use = "a rejected consumer-group member removal must become an operation input"]
#[derive(Debug)]
pub(crate) struct RemoveConsumerGroupMembersCallAdmissionFailure {
    source: RemoveConsumerGroupMembersCallAdmissionSource,
    plan: RemoveConsumerGroupMembersPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

#[derive(Debug)]
enum RemoveConsumerGroupMembersCallAdmissionSource {
    Request(RemoveConsumerGroupMembersRequestFailure),
    Driver(RemoveConsumerGroupMembersSubmitError),
}

impl RemoveConsumerGroupMembersCallAdmissionFailure {
    const fn request(
        source: RemoveConsumerGroupMembersRequestFailure,
        plan: RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            source: RemoveConsumerGroupMembersCallAdmissionSource::Request(source),
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    const fn driver(
        source: RemoveConsumerGroupMembersSubmitError,
        plan: RemoveConsumerGroupMembersPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            source: RemoveConsumerGroupMembersCallAdmissionSource::Driver(source),
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(crate) fn into_correlation(self) -> (RemoveConsumerGroupMembersPlan, usize, usize) {
        let Self {
            source,
            plan,
            request_scratch_limit,
            result_limit,
        } = self;
        drop(source);
        (plan, request_scratch_limit, result_limit)
    }
}

impl fmt::Display for RemoveConsumerGroupMembersCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            RemoveConsumerGroupMembersCallAdmissionSource::Request(source) => {
                write!(
                    formatter,
                    "member-removal LeaveGroup request rejected: {source}"
                )
            }
            RemoveConsumerGroupMembersCallAdmissionSource::Driver(source) => {
                write!(formatter, "{source}")
            }
        }
    }
}

impl Error for RemoveConsumerGroupMembersCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            RemoveConsumerGroupMembersCallAdmissionSource::Request(source) => Some(source),
            RemoveConsumerGroupMembersCallAdmissionSource::Driver(source) => Some(source),
        }
    }
}
