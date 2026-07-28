//! Linear ownership of one accepted exact-broker `AlterReplicaLogDirs` call.

mod evidence;

use core::mem::size_of;
use std::time::Instant;

use kafka_client_core::AlterReplicaLogDirAssignment;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterReplicaLogDirsResponse;

use crate::protocol::admin::alter_replica_log_dirs::{
    AlterReplicaLogDirAssignmentRef, alter_replica_log_dirs_request,
};

use super::{
    super::DriverOwner,
    alter_replica_log_dirs_terminal::{
        AlterReplicaLogDirsRawTerminal, RecoveredAlterReplicaLogDirsCall,
        retain_alter_replica_log_dirs_terminal,
    },
};

pub(super) use evidence::AlterReplicaLogDirsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted AlterReplicaLogDirs call must be terminally settled"]
pub(crate) struct AlterReplicaLogDirsCall {
    call: Option<RoutedCall<AlterReplicaLogDirsResponse>>,
    evidence: Option<AlterReplicaLogDirsEvidence>,
}

impl AlterReplicaLogDirsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, AlterReplicaLogDirsCallAdmissionFailure> {
        let evidence = AlterReplicaLogDirsEvidence::new(
            broker_id,
            assignments,
            request_scratch_limit,
            result_limit,
        );
        let (assignment_refs, request_limit) =
            match assignment_refs(evidence.assignments(), request_scratch_limit) {
                Ok(prepared) => prepared,
                Err(source) => {
                    return Err(AlterReplicaLogDirsCallAdmissionFailure::new(
                        source, evidence,
                    ));
                }
            };
        let request = alter_replica_log_dirs_request(&assignment_refs, request_limit);
        drop(assignment_refs);
        let request = match request {
            Ok(request) => request,
            Err(_source) => {
                return Err(AlterReplicaLogDirsCallAdmissionFailure::new(
                    AlterReplicaLogDirsCallAdmissionSource::Request,
                    evidence,
                ));
            }
        };
        let call = match driver.submit_tracked_alter_replica_log_dirs(
            evidence.broker_id(),
            request,
            deadline,
        ) {
            Ok(call) => call,
            Err(_source) => {
                return Err(AlterReplicaLogDirsCallAdmissionFailure::new(
                    AlterReplicaLogDirsCallAdmissionSource::Driver,
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }
}

fn assignment_refs(
    assignments: &[AlterReplicaLogDirAssignment],
    retained_limit: usize,
) -> Result<(Vec<AlterReplicaLogDirAssignmentRef<'_>>, usize), AlterReplicaLogDirsCallAdmissionSource>
{
    let assignment_ref_bytes = assignments
        .len()
        .checked_mul(size_of::<AlterReplicaLogDirAssignmentRef<'_>>())
        .ok_or(AlterReplicaLogDirsCallAdmissionSource::Request)?;
    retained_limit
        .checked_sub(assignment_ref_bytes)
        .ok_or(AlterReplicaLogDirsCallAdmissionSource::Request)?;
    let mut assignment_refs = Vec::new();
    assignment_refs
        .try_reserve_exact(assignments.len())
        .map_err(|_| AlterReplicaLogDirsCallAdmissionSource::Request)?;
    for assignment in assignments {
        assignment_refs.push(AlterReplicaLogDirAssignmentRef::new(
            assignment.topic(),
            assignment.partition(),
            assignment.log_dir(),
        ));
    }
    let assignment_ref_bytes = assignment_refs
        .capacity()
        .checked_mul(size_of::<AlterReplicaLogDirAssignmentRef<'_>>())
        .ok_or(AlterReplicaLogDirsCallAdmissionSource::Request)?;
    let request_limit = retained_limit
        .checked_sub(assignment_ref_bytes)
        .ok_or(AlterReplicaLogDirsCallAdmissionSource::Request)?;
    Ok((assignment_refs, request_limit))
}

impl AlterReplicaLogDirsCall {
    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterReplicaLogDirsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_alter_replica_log_dirs_terminal(
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
        broker_id: i32,
        assignments: &[AlterReplicaLogDirAssignment],
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence.as_ref().is_some_and(|evidence| {
            evidence.matches(broker_id, assignments, request_scratch_limit, result_limit)
        })
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredAlterReplicaLogDirsCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredAlterReplicaLogDirsCall::new(evidence)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlterReplicaLogDirsCallAdmissionSource {
    Request,
    Driver,
}

/// Definitely-unsent exact-route construction or bounded-driver rejection.
#[must_use = "a rejected AlterReplicaLogDirs call must become operation input"]
pub(crate) struct AlterReplicaLogDirsCallAdmissionFailure {
    source: AlterReplicaLogDirsCallAdmissionSource,
    evidence: AlterReplicaLogDirsEvidence,
}

impl AlterReplicaLogDirsCallAdmissionFailure {
    const fn new(
        source: AlterReplicaLogDirsCallAdmissionSource,
        evidence: AlterReplicaLogDirsEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_evidence(self) -> (i32, Vec<AlterReplicaLogDirAssignment>, usize, usize) {
        let Self { source, evidence } = self;
        let _ = source;
        evidence.into_parts()
    }
}
