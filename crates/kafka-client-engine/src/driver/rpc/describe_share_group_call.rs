//! Linear ownership of one accepted group-coordinator API-77 call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::{DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES, DescribeShareGroupPlan};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ShareGroupDescribeResponse;

use crate::protocol::admin::describe_share_group::{
    DescribeShareGroupRequestFailure, describe_share_group_request,
};

use super::{
    super::DriverOwner,
    describe_share_group_submission::DescribeShareGroupSubmitError,
    describe_share_group_terminal::{
        DescribeShareGroupTerminal, RecoveredDescribeShareGroupCall,
        retain_describe_share_group_terminal,
    },
};

/// One accepted read-only call retained beside its concrete operation owner.
#[must_use = "an accepted share-group description must be terminally settled"]
pub(crate) struct DescribeShareGroupCall {
    call: Option<RoutedCall<ShareGroupDescribeResponse>>,
}

impl DescribeShareGroupCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &DescribeShareGroupPlan,
        deadline: Instant,
    ) -> Result<Self, DescribeShareGroupCallAdmissionFailure> {
        let request = describe_share_group_request(
            plan.group_id(),
            plan.include_authorized_operations(),
            DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES,
        )
        .map_err(DescribeShareGroupCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_describe_share_group(plan.group_id(), request, deadline)
            .map_err(DescribeShareGroupCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeShareGroupTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_share_group_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeShareGroupCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeShareGroupCall::new()
        })
    }
}

/// Definitely-unsent request-construction or driver-admission rejection.
#[derive(Debug)]
#[must_use = "a rejected share-group description must become operation input"]
pub(crate) enum DescribeShareGroupCallAdmissionFailure {
    Request(DescribeShareGroupRequestFailure),
    Driver(DescribeShareGroupSubmitError),
}

impl fmt::Display for DescribeShareGroupCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => {
                write!(formatter, "ShareGroupDescribe request rejected: {source:?}")
            }
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for DescribeShareGroupCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(_) => None,
            Self::Driver(source) => Some(source),
        }
    }
}
