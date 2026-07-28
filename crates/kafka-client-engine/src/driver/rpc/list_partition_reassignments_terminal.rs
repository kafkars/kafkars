//! Neutral borrowed terminals with exact reassignment-query correlation.

use kafka_client_core::{
    DeliveryStatus, ListPartitionReassignmentsInput, ListPartitionReassignmentsPlan,
};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::ListPartitionReassignmentsResponse;

use super::{
    super::request_failure_delivery,
    reassignment_controller_refresh::{
        ReassignmentControllerRefresh, ReassignmentControllerRefreshPrepareError,
    },
};

/// Stable engine-local classification without exposing driver error variants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ListPartitionReassignmentsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure fact safe for its concrete Admin host.
pub(crate) enum ListPartitionReassignmentsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a ListPartitionReassignmentsResponse,
    },
    Failed {
        kind: ListPartitionReassignmentsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until borrowed protocol validation completes.
#[must_use = "a raw reassignment-listing terminal owns unsettled route evidence"]
pub(crate) struct ListPartitionReassignmentsRawTerminal {
    plan: ListPartitionReassignmentsPlan,
    result_limit: usize,
    selected_version: Option<i16>,
    result: Result<ListPartitionReassignmentsResponse, RequestError>,
    input: Option<ListPartitionReassignmentsInput>,
    controller_refresh: ReassignmentControllerRefresh,
}

impl ListPartitionReassignmentsRawTerminal {
    pub(crate) fn fact(&self) -> ListPartitionReassignmentsTerminalFact<'_> {
        match &self.result {
            Ok(response) => ListPartitionReassignmentsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => ListPartitionReassignmentsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn matches(
        &self,
        expected_plan: &ListPartitionReassignmentsPlan,
        expected_result_limit: usize,
    ) -> bool {
        self.plan == *expected_plan && self.result_limit == expected_result_limit
    }

    pub(crate) fn prepare_input(
        &mut self,
        input: ListPartitionReassignmentsInput,
    ) -> Result<(), ReassignmentControllerRefreshPrepareError> {
        if self.input.is_some() {
            return Err(ReassignmentControllerRefreshPrepareError::AlreadyPrepared);
        }
        self.controller_refresh
            .prepare(input_requires_controller_refresh(&input))?;
        self.input = Some(input);
        Ok(())
    }

    pub(crate) const fn controller_refresh_pending(&self) -> bool {
        self.controller_refresh.is_pending()
    }

    pub(crate) const fn input_prepared(&self) -> bool {
        self.input.is_some()
    }

    pub(crate) fn poll_controller_refresh(&mut self, driver: &super::super::DriverOwner) -> bool {
        self.controller_refresh.poll(driver)
    }

    pub(crate) fn take_input(&mut self) -> Option<ListPartitionReassignmentsInput> {
        self.input.take()
    }

    pub(crate) fn discard_controller_refresh_after_driver_shutdown(&mut self) {
        let refresh = std::mem::replace(
            &mut self.controller_refresh,
            ReassignmentControllerRefresh::unclassified(None),
        );
        refresh.discard_after_driver_shutdown();
        self.controller_refresh
            .prepare(false)
            .unwrap_or_else(|error| unreachable!("fresh replacement must prepare: {error:?}"));
    }

    /// Deliberately releases route evidence only after deterministic settlement.
    pub(crate) fn discard(self) {
        let Self {
            plan,
            result,
            input,
            controller_refresh,
            ..
        } = self;
        drop(plan);
        drop(result);
        drop(input);
        drop(controller_refresh);
    }

    #[cfg(test)]
    pub(crate) fn for_test(plan: ListPartitionReassignmentsPlan, result_limit: usize) -> Self {
        retain_list_partition_reassignments_terminal(
            plan,
            result_limit,
            Some(ApiVersion::new(0)),
            Ok(ListPartitionReassignmentsResponse::default()),
            None,
        )
    }
}

pub(super) fn retain_list_partition_reassignments_terminal(
    plan: ListPartitionReassignmentsPlan,
    result_limit: usize,
    selected_version: Option<ApiVersion>,
    result: Result<ListPartitionReassignmentsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
) -> ListPartitionReassignmentsRawTerminal {
    ListPartitionReassignmentsRawTerminal {
        plan,
        result_limit,
        selected_version: selected_version.map(ApiVersion::value),
        result,
        input: None,
        controller_refresh: ReassignmentControllerRefresh::unclassified(route_token),
    }
}

pub(super) fn input_requires_controller_refresh(input: &ListPartitionReassignmentsInput) -> bool {
    matches!(
        input,
        ListPartitionReassignmentsInput::BrokerRejected { error } if error.code() == 41
    )
}

fn failure_kind(error: &RequestError) -> ListPartitionReassignmentsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => ListPartitionReassignmentsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => ListPartitionReassignmentsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            ListPartitionReassignmentsDriverFailureKind::Compatibility
        }
        _ => ListPartitionReassignmentsDriverFailureKind::Transport,
    }
}

/// Accepted call ownership recovered only after driver shutdown.
#[must_use = "recovered reassignment-listing ownership still requires core settlement"]
pub(crate) struct RecoveredListPartitionReassignmentsCall {
    plan: ListPartitionReassignmentsPlan,
    result_limit: usize,
}

impl RecoveredListPartitionReassignmentsCall {
    pub(super) const fn new(plan: ListPartitionReassignmentsPlan, result_limit: usize) -> Self {
        Self { plan, result_limit }
    }

    #[cfg(test)]
    pub(crate) const fn for_test(
        plan: ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> Self {
        Self::new(plan, result_limit)
    }

    pub(crate) fn matches(
        &self,
        expected_plan: &ListPartitionReassignmentsPlan,
        expected_result_limit: usize,
    ) -> bool {
        self.plan == *expected_plan && self.result_limit == expected_result_limit
    }

    /// Consumes recovered call ownership after core receives its terminal fact.
    pub(crate) fn seal(self) {
        let Self { plan, .. } = self;
        drop(plan);
    }
}
