//! Atomic processing, Fetch, catalog, and event retirement for assignment loss.

use kafka_client_core::{ClassicGeneration, ClassicProcessingLease, LiveGroupAssignment};

use super::{
    super::{
        classic_group_fetch::{ClassicGroupFetchOwner, ClassicGroupFetchRetirement},
        classic_group_owner::ClassicGroupOwner,
        session_catalog::GroupSessionCatalog,
    },
    ClassicGroupRevocationFailure, ClassicGroupRevocationFailureKind,
};

#[expect(
    clippy::result_large_err,
    reason = "rejection returns the exact linear assignment and generation without heap indirection"
)]
pub(in crate::consumer::group) fn retire_and_revoke_classic_group_assignment(
    owner: &ClassicGroupOwner,
    catalog: &mut GroupSessionCatalog,
    processing_lease: &mut ClassicProcessingLease,
    fetch: &mut ClassicGroupFetchOwner,
    assignment: LiveGroupAssignment,
    classic_generation: ClassicGeneration,
) -> Result<ClassicGroupFetchRetirement, ClassicGroupRevocationFailure> {
    let retirement_phase = owner.machine().phase();
    let assignment_epoch = assignment.assignment_generation().get();
    let event = catalog.prepare_assignment_retirement_event(&assignment);
    let processing_fence = processing_lease
        .active_schedule()
        .map(kafka_client_core::ClassicProcessingLeaseSchedule::fence)
        .or_else(|| {
            processing_lease
                .pending_expiration()
                .map(|expiration| expiration.schedule().fence())
        });
    let prepared = owner
        .prepare_revoke(catalog, assignment, classic_generation)
        .map_err(|failure| ClassicGroupRevocationFailure {
            kind: ClassicGroupRevocationFailureKind::Catalog(failure.kind),
            assignment: failure.assignment,
            classic_generation,
        })?;
    let Some(retained_processing_fence) = processing_fence else {
        return Err(ClassicGroupRevocationFailure {
            kind: ClassicGroupRevocationFailureKind::ProcessingLeaseCycleUnavailable,
            assignment: prepared.assignment,
            classic_generation: prepared.classic_generation,
        });
    };
    let processing_fence = kafka_client_core::ClassicProcessingLeaseFence::new(
        prepared.assignment.group_id(),
        retained_processing_fence.cycle(),
        prepared.assignment.assignment_generation(),
    );
    let processing_revocation = match processing_lease.prepare_revocation(processing_fence) {
        Ok(prepared_revocation) => prepared_revocation,
        Err(error) => {
            return Err(ClassicGroupRevocationFailure {
                kind: ClassicGroupRevocationFailureKind::ProcessingLease(error),
                assignment: prepared.assignment,
                classic_generation: prepared.classic_generation,
            });
        }
    };
    let retirement = match fetch.retire_for_assignment_loss(&prepared.assignment) {
        Ok(retirement) => retirement,
        Err(error) => {
            return Err(ClassicGroupRevocationFailure {
                kind: ClassicGroupRevocationFailureKind::Fetch(error),
                assignment: prepared.assignment,
                classic_generation: prepared.classic_generation,
            });
        }
    };
    let _transition = processing_revocation.commit();
    prepared.commit();
    catalog.commit_assignment_retirement_event(event, assignment_epoch, retirement_phase);
    Ok(retirement)
}
