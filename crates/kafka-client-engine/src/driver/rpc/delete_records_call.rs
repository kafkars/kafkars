//! Linear ownership of one accepted leader-routed Admin `DeleteRecords` call.

use std::time::Instant;

use kafka_client_core::DeleteRecordsTarget;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DeleteRecordsResponse;

use crate::protocol::admin::delete_records::delete_records_request;

use super::{
    super::DriverOwner,
    delete_records_terminal::{
        DeleteRecordsRawTerminal, RecoveredDeleteRecordsCall, retain_delete_records_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted Admin DeleteRecords call must be terminally settled"]
pub(crate) struct DeleteRecordsCall {
    call: Option<RoutedCall<DeleteRecordsResponse>>,
}

impl DeleteRecordsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        target: &DeleteRecordsTarget,
        timeout_ms: i32,
        deadline: Instant,
    ) -> Result<Self, DeleteRecordsCallAdmissionFailure> {
        let request = delete_records_request(target, timeout_ms)
            .map_err(|_source| DeleteRecordsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_delete_records(target.topic(), target.partition(), request, deadline)
            .map_err(|_source| DeleteRecordsCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DeleteRecordsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_delete_records_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDeleteRecordsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDeleteRecordsCall::new()
        })
    }
}

/// Definitely-unsent failure from request construction or driver admission.
#[must_use = "a rejected Admin DeleteRecords call must become an operation input"]
pub(crate) enum DeleteRecordsCallAdmissionFailure {
    Request,
    Driver,
}
