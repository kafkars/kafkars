//! Linear ownership of one accepted group-coordinator `OffsetDelete` call.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::OffsetDeleteResponse;

use crate::protocol::admin::group_offset_delete::{
    GroupOffsetDeleteRequestFailure, OffsetDeleteTargetRef, group_offset_delete_request,
};

use super::{
    super::DriverOwner,
    group_offset_delete_submission::GroupOffsetDeleteSubmitError,
    group_offset_delete_terminal::{
        GroupOffsetDeleteTerminal, RecoveredGroupOffsetDeleteCall,
        retain_group_offset_delete_terminal,
    },
};

/// One accepted driver call retained beside its future concrete operation owner.
#[must_use = "an accepted group-offset deletion call must be terminally settled"]
pub(crate) struct GroupOffsetDeleteCall {
    call: Option<RoutedCall<OffsetDeleteResponse>>,
}

impl GroupOffsetDeleteCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        group: &str,
        targets: &[OffsetDeleteTargetRef<'_>],
        request_scratch_limit: usize,
        deadline: Instant,
    ) -> Result<Self, GroupOffsetDeleteCallAdmissionFailure> {
        let request = group_offset_delete_request(group, targets, request_scratch_limit)
            .map_err(GroupOffsetDeleteCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_group_offset_delete(group, request, deadline)
            .map_err(GroupOffsetDeleteCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts a ready terminal once without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<GroupOffsetDeleteTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_group_offset_delete_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals an unresolved accepted call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredGroupOffsetDeleteCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredGroupOffsetDeleteCall::new()
        })
    }
}

/// Definitely-unsent rejection from coordinator validation or bounded driver admission.
#[must_use = "a rejected group-offset deletion call must become an operation input"]
#[derive(Debug)]
pub(crate) enum GroupOffsetDeleteCallAdmissionFailure {
    Request(GroupOffsetDeleteRequestFailure),
    Driver(GroupOffsetDeleteSubmitError),
}

impl fmt::Display for GroupOffsetDeleteCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => write!(formatter, "OffsetDelete request rejected: {source}"),
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for GroupOffsetDeleteCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}
