//! Linear ownership of one accepted group-coordinator API-89 call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::{DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES, DescribeStreamsGroupPlan};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::StreamsGroupDescribeResponse;

use crate::protocol::admin::describe_streams_group::{
    DescribeStreamsGroupRequestFailure, describe_streams_group_request,
};

use super::{
    super::DriverOwner,
    describe_streams_group_submission::DescribeStreamsGroupSubmitError,
    describe_streams_group_terminal::{
        DescribeStreamsGroupTerminal, RecoveredDescribeStreamsGroupCall,
        retain_describe_streams_group_terminal,
    },
};

/// One accepted read-only call retained beside its concrete operation owner.
#[must_use = "an accepted streams-group description must be terminally settled"]
pub(crate) struct DescribeStreamsGroupCall {
    call: Option<RoutedCall<StreamsGroupDescribeResponse>>,
}

impl DescribeStreamsGroupCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &DescribeStreamsGroupPlan,
        deadline: Instant,
    ) -> Result<Self, DescribeStreamsGroupCallAdmissionFailure> {
        let request = describe_streams_group_request(
            plan.group_id(),
            plan.include_authorized_operations(),
            plan.include_topology_description(),
            DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES,
        )
        .map_err(DescribeStreamsGroupCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_describe_streams_group(plan.group_id(), request, deadline)
            .map_err(DescribeStreamsGroupCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeStreamsGroupTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_streams_group_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeStreamsGroupCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeStreamsGroupCall::new()
        })
    }
}

/// Definitely-unsent request-construction or driver-admission rejection.
#[derive(Debug)]
#[must_use = "a rejected streams-group description must become operation input"]
pub(crate) enum DescribeStreamsGroupCallAdmissionFailure {
    Request(DescribeStreamsGroupRequestFailure),
    Driver(DescribeStreamsGroupSubmitError),
}

impl fmt::Display for DescribeStreamsGroupCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => {
                write!(
                    formatter,
                    "StreamsGroupDescribe request rejected: {source:?}"
                )
            }
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for DescribeStreamsGroupCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(_) => None,
            Self::Driver(source) => Some(source),
        }
    }
}
