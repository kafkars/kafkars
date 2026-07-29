//! Linear ownership of one accepted group-coordinator API-91 call.

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::AlterShareGroupOffsetsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterShareGroupOffsetsResponse;

use crate::protocol::admin::alter_share_group_offsets::{
    AlterShareGroupOffsetsRequestFailure, alter_share_group_offsets_request,
};

use super::{
    super::DriverOwner,
    alter_share_group_offsets_submission::AlterShareGroupOffsetsSubmitError,
    alter_share_group_offsets_terminal::{
        AlterShareGroupOffsetsTerminal, RecoveredAlterShareGroupOffsetsCall,
        retain_alter_share_group_offsets_terminal,
    },
};

/// One accepted destructive call retained beside its concrete operation owner.
#[must_use = "an accepted share-group offset alteration must be terminally settled"]
pub(crate) struct AlterShareGroupOffsetsCall {
    call: Option<RoutedCall<AlterShareGroupOffsetsResponse>>,
}

impl AlterShareGroupOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: &AlterShareGroupOffsetsPlan,
        deadline: Instant,
    ) -> Result<Self, AlterShareGroupOffsetsCallAdmissionFailure> {
        let request = alter_share_group_offsets_request(plan)
            .map_err(AlterShareGroupOffsetsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_alter_share_group_offsets(plan.group_id(), request, deadline)
            .map_err(AlterShareGroupOffsetsCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterShareGroupOffsetsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_alter_share_group_offsets_terminal(
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
    ) -> Option<RecoveredAlterShareGroupOffsetsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredAlterShareGroupOffsetsCall::new()
        })
    }
}

/// Definitely-unsent request-construction or driver-admission rejection.
#[derive(Debug)]
#[must_use = "a rejected share-group offset alteration must become operation input"]
pub(crate) enum AlterShareGroupOffsetsCallAdmissionFailure {
    Request(AlterShareGroupOffsetsRequestFailure),
    Driver(AlterShareGroupOffsetsSubmitError),
}

impl fmt::Display for AlterShareGroupOffsetsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => {
                write!(
                    formatter,
                    "AlterShareGroupOffsets request rejected: {source}"
                )
            }
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for AlterShareGroupOffsetsCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(source) => Some(source),
            Self::Driver(source) => Some(source),
        }
    }
}
