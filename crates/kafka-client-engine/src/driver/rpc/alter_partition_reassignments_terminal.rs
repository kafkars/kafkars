//! Raw reassignment terminal facts with linear route-receipt ownership.

use kafka_client_core::{
    AlterPartitionReassignmentResult, AlterPartitionReassignmentsInput, DeliveryStatus,
};
use kafka_driver::{ApiVersion, CallFailure, RequestError, RouteFailureToken};
use kafka_wire::AlterPartitionReassignmentsResponse;

use super::{
    super::request_failure_delivery,
    alter_partition_reassignments_call::AlterPartitionReassignmentsEvidence,
    reassignment_controller_refresh::{
        ReassignmentControllerRefresh, ReassignmentControllerRefreshPrepareError,
    },
};

/// Stable engine-local failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterPartitionReassignmentsDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Borrowed success or failure safe for the concrete host interpreter.
pub(crate) enum AlterPartitionReassignmentsTerminalFact<'a> {
    Response {
        selected_version: Option<i16>,
        response: &'a AlterPartitionReassignmentsResponse,
    },
    Failed {
        kind: AlterPartitionReassignmentsDriverFailureKind,
        delivery: DeliveryStatus,
    },
}

/// Uninterpreted terminal retained until deterministic settlement.
#[must_use = "a raw reassignment terminal owns unsettled route evidence"]
pub(crate) struct AlterPartitionReassignmentsTerminal {
    selected_version: Option<i16>,
    result: Result<AlterPartitionReassignmentsResponse, RequestError>,
    input: Option<AlterPartitionReassignmentsInput>,
    controller_refresh: ReassignmentControllerRefresh,
    evidence: AlterPartitionReassignmentsEvidence,
}

impl AlterPartitionReassignmentsTerminal {
    pub(crate) fn matches_evidence(
        &self,
        plan: &kafka_client_core::AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(plan, request_scratch_limit, result_limit)
    }

    pub(crate) const fn response_plan(
        &self,
    ) -> &kafka_client_core::AlterPartitionReassignmentsPlan {
        self.evidence.plan()
    }

    pub(crate) const fn result_limit(&self) -> usize {
        self.evidence.result_limit()
    }

    pub(crate) fn fact(&self) -> AlterPartitionReassignmentsTerminalFact<'_> {
        match &self.result {
            Ok(response) => AlterPartitionReassignmentsTerminalFact::Response {
                selected_version: self.selected_version,
                response,
            },
            Err(error) => AlterPartitionReassignmentsTerminalFact::Failed {
                kind: failure_kind(error),
                delivery: request_failure_delivery(error),
            },
        }
    }

    pub(crate) fn prepare_input(
        &mut self,
        input: AlterPartitionReassignmentsInput,
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

    pub(crate) fn take_input(&mut self) -> Option<AlterPartitionReassignmentsInput> {
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

    /// Releases route evidence only after core terminal settlement.
    pub(crate) fn discard(self) {
        let Self {
            input,
            controller_refresh,
            evidence,
            ..
        } = self;
        drop((input, controller_refresh, evidence));
    }
}

pub(super) fn retain_alter_partition_reassignments_terminal(
    selected_version: Option<ApiVersion>,
    result: Result<AlterPartitionReassignmentsResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    evidence: AlterPartitionReassignmentsEvidence,
) -> AlterPartitionReassignmentsTerminal {
    AlterPartitionReassignmentsTerminal {
        selected_version: selected_version.map(ApiVersion::value),
        result,
        input: None,
        controller_refresh: ReassignmentControllerRefresh::unclassified(route_token),
        evidence,
    }
}

pub(super) fn input_requires_controller_refresh(input: &AlterPartitionReassignmentsInput) -> bool {
    match input {
        AlterPartitionReassignmentsInput::BrokerRejected { error } => error.code() == 41,
        AlterPartitionReassignmentsInput::BrokerResponded { batch } => {
            batch.outcomes().iter().any(|outcome| {
                matches!(
                    outcome.result(),
                    AlterPartitionReassignmentResult::Failed(error) if error.code() == 41
                )
            })
        }
        _ => false,
    }
}

fn failure_kind(error: &RequestError) -> AlterPartitionReassignmentsDriverFailureKind {
    match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => AlterPartitionReassignmentsDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => AlterPartitionReassignmentsDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            AlterPartitionReassignmentsDriverFailureKind::Compatibility
        }
        _ => AlterPartitionReassignmentsDriverFailureKind::Transport,
    }
}

/// Accepted call recovered only after the unique driver is destroyed.
#[must_use = "recovered reassignment ownership still requires core settlement"]
pub(crate) struct RecoveredAlterPartitionReassignmentsCall {
    evidence: AlterPartitionReassignmentsEvidence,
}

impl RecoveredAlterPartitionReassignmentsCall {
    pub(super) const fn new(evidence: AlterPartitionReassignmentsEvidence) -> Self {
        Self { evidence }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &kafka_client_core::AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .matches(plan, request_scratch_limit, result_limit)
    }

    pub(crate) fn seal(self) {
        drop(self.evidence);
    }
}
