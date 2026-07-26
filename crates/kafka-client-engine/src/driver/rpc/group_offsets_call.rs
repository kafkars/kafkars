//! Linear ownership of one accepted group-wide `OffsetFetch` call.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::OffsetFetchResponse;

use crate::protocol::admin::group_offsets::group_offsets_request;

use super::{
    super::DriverOwner,
    group_offsets_submission::GroupOffsetsSubmitError,
    group_offsets_terminal::{
        GroupOffsetsTerminal, RecoveredGroupOffsetsCall, retain_group_offsets_terminal,
    },
};

/// One accepted driver call retained beside its future admin operation owner.
#[must_use = "an accepted group-offset call must be terminally settled"]
pub(crate) struct GroupOffsetsCall {
    call: Option<RoutedCall<OffsetFetchResponse>>,
}

impl GroupOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        group: &str,
        require_stable: bool,
        deadline: Instant,
    ) -> Result<Self, GroupOffsetsCallAdmissionFailure> {
        let request = group_offsets_request(group, require_stable);
        let call = driver
            .submit_tracked_group_offsets(group, request, deadline, require_stable)
            .map_err(GroupOffsetsCallAdmissionFailure::new)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(&mut self) -> Option<Result<GroupOffsetsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_group_offsets_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals an unresolved accepted call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredGroupOffsetsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredGroupOffsetsCall::new()
        })
    }
}

/// Definitely-unsent rejection from coordinator validation or bounded driver admission.
#[must_use = "a rejected group-offset call must become an operation input"]
pub(crate) struct GroupOffsetsCallAdmissionFailure {
    source: GroupOffsetsSubmitError,
}

impl GroupOffsetsCallAdmissionFailure {
    const fn new(source: GroupOffsetsSubmitError) -> Self {
        Self { source }
    }

    pub(crate) fn into_source(self) -> GroupOffsetsSubmitError {
        self.source
    }
}
