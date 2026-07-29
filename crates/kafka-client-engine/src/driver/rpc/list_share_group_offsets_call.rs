//! Linear ownership of one accepted group-coordinator API-90 call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::ListShareGroupOffsetsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeShareGroupOffsetsResponse;

use crate::protocol::admin::list_share_group_offsets::{
    ListShareGroupOffsetsRequestFailure, list_share_group_offsets_request,
};

use super::{
    super::DriverOwner,
    list_share_group_offsets_submission::ListShareGroupOffsetsSubmitError,
    list_share_group_offsets_terminal::{
        ListShareGroupOffsetsTerminal, RecoveredListShareGroupOffsetsCall,
        retain_list_share_group_offsets_terminal,
    },
};

/// One accepted read-only call retained beside its concrete operation owner.
#[must_use = "an accepted share-group offset listing must be terminally settled"]
pub(crate) struct ListShareGroupOffsetsCall {
    call: Option<RoutedCall<DescribeShareGroupOffsetsResponse>>,
}

impl ListShareGroupOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &ListShareGroupOffsetsPlan,
        deadline: Instant,
    ) -> Result<Self, ListShareGroupOffsetsCallAdmissionFailure> {
        let request = list_share_group_offsets_request(plan)
            .map_err(ListShareGroupOffsetsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_list_share_group_offsets(plan.group_id(), request, deadline)
            .map_err(ListShareGroupOffsetsCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready terminal without blocking or releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListShareGroupOffsetsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_list_share_group_offsets_terminal(
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
    ) -> Option<RecoveredListShareGroupOffsetsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredListShareGroupOffsetsCall::new()
        })
    }
}

/// Definitely-unsent driver admission rejection.
#[derive(Debug)]
#[must_use = "a rejected share-group offset listing must become operation input"]
pub(crate) enum ListShareGroupOffsetsCallAdmissionFailure {
    Request(ListShareGroupOffsetsRequestFailure),
    Driver(ListShareGroupOffsetsSubmitError),
}

impl fmt::Display for ListShareGroupOffsetsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => write!(formatter, "API-90 request rejected: {source}"),
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for ListShareGroupOffsetsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}
