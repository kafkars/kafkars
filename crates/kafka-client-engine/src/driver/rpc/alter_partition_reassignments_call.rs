//! Linear ownership of one accepted controller reassignment call.

mod evidence;

use std::{error::Error, fmt};

use kafka_client_core::{AlterPartitionReassignmentsPlan, Moment};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterPartitionReassignmentsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::alter_partition_reassignments::{
        AlterPartitionReassignmentRef, AlterPartitionReassignmentsDeadlineError,
        AlterPartitionReassignmentsRequestFailure, alter_partition_reassignments_request,
        remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    alter_partition_reassignments_submission::AlterPartitionReassignmentsSubmitError,
    alter_partition_reassignments_terminal::{
        AlterPartitionReassignmentsTerminal, RecoveredAlterPartitionReassignmentsCall,
        retain_alter_partition_reassignments_terminal,
    },
};

pub(super) use evidence::AlterPartitionReassignmentsEvidence;

/// One accepted driver call retained beside its concrete operation owner.
#[must_use = "an accepted reassignment call must be terminally settled"]
pub(crate) struct AlterPartitionReassignmentsCall {
    call: Option<RoutedCall<AlterPartitionReassignmentsResponse>>,
    evidence: Option<AlterPartitionReassignmentsEvidence>,
}

impl AlterPartitionReassignmentsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: OperationDeadline,
        now: Moment,
    ) -> Result<Self, AlterPartitionReassignmentsCallAdmissionFailure> {
        let evidence =
            AlterPartitionReassignmentsEvidence::new(plan, request_scratch_limit, result_limit);
        let timeout_ms = match remaining_timeout_ms(now, deadline.core()) {
            Ok(timeout_ms) => timeout_ms,
            Err(source) => {
                return Err(AlterPartitionReassignmentsCallAdmissionFailure::new(
                    AlterPartitionReassignmentsAdmissionSource::Deadline(source),
                    evidence,
                ));
            }
        };
        let changes = match change_refs(evidence.plan()) {
            Ok(changes) => changes,
            Err(source) => {
                return Err(AlterPartitionReassignmentsCallAdmissionFailure::new(
                    AlterPartitionReassignmentsAdmissionSource::Request(source),
                    evidence,
                ));
            }
        };
        let request = alter_partition_reassignments_request(
            &changes,
            evidence.plan().allow_replication_factor_change(),
            timeout_ms,
            request_scratch_limit,
        );
        drop(changes);
        let request = match request {
            Ok(request) => request,
            Err(source) => {
                return Err(AlterPartitionReassignmentsCallAdmissionFailure::new(
                    AlterPartitionReassignmentsAdmissionSource::Request(source),
                    evidence,
                ));
            }
        };
        let call = match driver
            .submit_tracked_alter_partition_reassignments(request, deadline.transport())
        {
            Ok(call) => call,
            Err(source) => {
                return Err(AlterPartitionReassignmentsCallAdmissionFailure::new(
                    AlterPartitionReassignmentsAdmissionSource::Driver(source),
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterPartitionReassignmentsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_alter_partition_reassignments_terminal(
                    selected_version,
                    result,
                    route_token,
                    evidence,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &AlterPartitionReassignmentsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, request_scratch_limit, result_limit))
    }

    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredAlterPartitionReassignmentsCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredAlterPartitionReassignmentsCall::new(evidence)
        })
    }
}

fn change_refs(
    plan: &AlterPartitionReassignmentsPlan,
) -> Result<Vec<AlterPartitionReassignmentRef<'_>>, AlterPartitionReassignmentsRequestFailure> {
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(plan.changes().len())
        .map_err(|_| AlterPartitionReassignmentsRequestFailure::RetainedBytes)?;
    changes.extend(plan.changes().iter().map(|change| {
        AlterPartitionReassignmentRef::new(
            change.topic(),
            change.partition(),
            change.target().replicas(),
        )
    }));
    Ok(changes)
}

/// Definitely-unsent rejection before tracked driver ownership.
#[derive(Debug)]
enum AlterPartitionReassignmentsAdmissionSource {
    Deadline(AlterPartitionReassignmentsDeadlineError),
    Request(AlterPartitionReassignmentsRequestFailure),
    Driver(AlterPartitionReassignmentsSubmitError),
}

/// Definitely-unsent rejection retaining the exact attempted submission.
#[derive(Debug)]
pub(crate) struct AlterPartitionReassignmentsCallAdmissionFailure {
    source: AlterPartitionReassignmentsAdmissionSource,
    evidence: AlterPartitionReassignmentsEvidence,
}

impl AlterPartitionReassignmentsCallAdmissionFailure {
    const fn new(
        source: AlterPartitionReassignmentsAdmissionSource,
        evidence: AlterPartitionReassignmentsEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_submission_evidence(
        self,
    ) -> (AlterPartitionReassignmentsPlan, usize, usize) {
        let Self { source, evidence } = self;
        drop(source);
        evidence.into_parts()
    }
}

impl fmt::Display for AlterPartitionReassignmentsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            AlterPartitionReassignmentsAdmissionSource::Deadline(source) => {
                write!(formatter, "request deadline rejected: {source:?}")
            }
            AlterPartitionReassignmentsAdmissionSource::Request(source) => {
                write!(formatter, "request rejected: {source}")
            }
            AlterPartitionReassignmentsAdmissionSource::Driver(source) => {
                write!(formatter, "{source}")
            }
        }
    }
}

impl Error for AlterPartitionReassignmentsCallAdmissionFailure {}
