//! Linear ownership of one accepted leader-routed Admin `ListOffsets` call.

use std::time::Instant;

use kafka_client_core::{AdminListOffsetTarget, ReadIsolation};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ListOffsetsResponse;

use crate::protocol::admin::list_offsets::admin_list_offsets_request;

use super::{
    super::DriverOwner,
    admin_list_offsets_terminal::{
        AdminListOffsetsTerminal, RecoveredAdminListOffsetsCall, retain_admin_list_offsets_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted Admin ListOffsets call must be terminally settled"]
pub(crate) struct AdminListOffsetsCall {
    call: Option<RoutedCall<ListOffsetsResponse>>,
    target: Option<AdminListOffsetTarget>,
    read_isolation: Option<ReadIsolation>,
}

impl AdminListOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        target: AdminListOffsetTarget,
        read_isolation: ReadIsolation,
        timeout_ms: i32,
        deadline: Instant,
    ) -> Result<Self, AdminListOffsetsCallAdmissionFailure> {
        let request = match admin_list_offsets_request(&target, read_isolation, timeout_ms) {
            Ok(request) => request,
            Err(_source) => {
                return Err(AdminListOffsetsCallAdmissionFailure {
                    target,
                    read_isolation,
                });
            }
        };
        let call = match driver.submit_tracked_admin_list_offsets(
            &target,
            read_isolation,
            request,
            deadline,
        ) {
            Ok(call) => call,
            Err(_source) => {
                return Err(AdminListOffsetsCallAdmissionFailure {
                    target,
                    read_isolation,
                });
            }
        };
        Ok(Self {
            call: Some(call),
            target: Some(target),
            read_isolation: Some(read_isolation),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AdminListOffsetsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let target = self.target.take()?;
                let read_isolation = self.read_isolation.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_admin_list_offsets_terminal(
                    selected_version,
                    result,
                    route_token,
                    target,
                    read_isolation,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches_correlation(
        &self,
        expected_target: &AdminListOffsetTarget,
        expected_read_isolation: ReadIsolation,
    ) -> bool {
        self.target.as_ref() == Some(expected_target)
            && self.read_isolation == Some(expected_read_isolation)
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredAdminListOffsetsCall> {
        let Self {
            call,
            target,
            read_isolation,
        } = self;
        match (call, target, read_isolation) {
            (Some(call), Some(target), Some(read_isolation)) => {
                drop(call);
                Some(RecoveredAdminListOffsetsCall::new(target, read_isolation))
            }
            _ => None,
        }
    }
}

/// Definitely-unsent failure from request construction or driver admission.
#[must_use = "a rejected Admin ListOffsets call must become an operation input"]
pub(crate) struct AdminListOffsetsCallAdmissionFailure {
    target: AdminListOffsetTarget,
    read_isolation: ReadIsolation,
}

impl AdminListOffsetsCallAdmissionFailure {
    pub(crate) fn into_correlation(self) -> (AdminListOffsetTarget, ReadIsolation) {
        (self.target, self.read_isolation)
    }
}
