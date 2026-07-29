//! Linear ownership of one accepted group-coordinator API-92 call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::DeleteShareGroupOffsetsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DeleteShareGroupOffsetsResponse;

use crate::protocol::admin::delete_share_group_offsets::{
    DeleteShareGroupOffsetsRequestFailure, delete_share_group_offsets_request,
};

use super::{
    super::DriverOwner,
    delete_share_group_offsets_submission::DeleteShareGroupOffsetsSubmitError,
    delete_share_group_offsets_terminal::{
        DeleteShareGroupOffsetsTerminal, RecoveredDeleteShareGroupOffsetsCall,
        retain_delete_share_group_offsets_terminal,
    },
};

/// One accepted destructive call retained beside its concrete operation owner.
#[must_use = "an accepted share-group offset deletion must be terminally settled"]
pub(crate) struct DeleteShareGroupOffsetsCall {
    call: Option<RoutedCall<DeleteShareGroupOffsetsResponse>>,
}

impl DeleteShareGroupOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &DeleteShareGroupOffsetsPlan,
        deadline: Instant,
    ) -> Result<Self, DeleteShareGroupOffsetsCallAdmissionFailure> {
        let request = delete_share_group_offsets_request(plan)
            .map_err(DeleteShareGroupOffsetsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_delete_share_group_offsets(plan.group_id(), request, deadline)
            .map_err(DeleteShareGroupOffsetsCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DeleteShareGroupOffsetsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_delete_share_group_offsets_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredDeleteShareGroupOffsetsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDeleteShareGroupOffsetsCall::new()
        })
    }
}

/// Definitely-unsent request-construction or driver-admission rejection.
#[derive(Debug)]
#[must_use = "a rejected share-group offset deletion must become operation input"]
pub(crate) enum DeleteShareGroupOffsetsCallAdmissionFailure {
    Request(DeleteShareGroupOffsetsRequestFailure),
    Driver(DeleteShareGroupOffsetsSubmitError),
}

impl fmt::Display for DeleteShareGroupOffsetsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => {
                write!(
                    formatter,
                    "DeleteShareGroupOffsets request rejected: {source}"
                )
            }
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for DeleteShareGroupOffsetsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}
