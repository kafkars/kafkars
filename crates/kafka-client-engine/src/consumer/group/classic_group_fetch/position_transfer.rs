//! Lossless transfer from confirmed group positions into Fetch activation.

use kafka_client_core::GroupPositionFence;

use super::{
    activation::{
        ClassicGroupFetchActivationError, ClassicGroupFetchActivationFailureKind,
        ClassicGroupFetchPostCoreFaultKind,
    },
    owner::ClassicGroupFetchOwner,
};
use crate::consumer::group::{
    classic_group_owner::ClassicGroupOwner,
    classic_group_position::{ClassicGroupPositionExecution, ClassicGroupPositionExecutionState},
    consumer_group_execution::ConsumerGroupExecution,
    session_catalog::GroupSessionCatalog,
};

/// Outcome of attempting one confirmed-position transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchTransferTurn {
    Idle,
    Activated,
}

/// Why the current membership owners could not supply one exact position fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchCurrentFenceError {
    MissingMembershipCycle,
    MissingClassicAssignment,
    MissingCatalogAssignment,
    CatalogGroupMismatch,
    AssignmentMismatch,
}

/// Stable location of every owner after a failed transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchTransferError {
    CurrentFence(ClassicGroupFetchCurrentFenceError),
    Returned(ClassicGroupFetchActivationFailureKind),
    Retained(ClassicGroupFetchPostCoreFaultKind),
}

/// Transfers one fully confirmed position owner into the Fetch owner exactly once.
///
/// A non-complete position lifecycle is inert. A complete owner is not removed
/// until the current classic machine and catalog prove the same live assignment
/// and exact membership cycle. Once handed to Fetch activation, every pre-core
/// rejection restores that same completed owner; only successful activation or
/// a Fetch-retained post-core fault leaves position execution dormant.
pub(in crate::consumer::group) fn transfer_completed_position(
    classic: &ClassicGroupOwner,
    catalog: &GroupSessionCatalog,
    position: &mut ClassicGroupPositionExecution,
    fetch: &mut ClassicGroupFetchOwner,
) -> Result<ClassicGroupFetchTransferTurn, ClassicGroupFetchTransferError> {
    if !position.has_ready_bootstrap_terminal() {
        return Ok(ClassicGroupFetchTransferTurn::Idle);
    }
    let current_fence = current_position_fence(classic, catalog)
        .map_err(ClassicGroupFetchTransferError::CurrentFence)?;
    transfer_completed_with_fence(position, fetch, current_fence)
}

/// Transfers one modern-group position owner after core and catalog agree.
pub(in crate::consumer::group) fn transfer_completed_consumer_group_position(
    consumer: &ConsumerGroupExecution,
    catalog: &GroupSessionCatalog,
    position: &mut ClassicGroupPositionExecution,
    fetch: &mut ClassicGroupFetchOwner,
) -> Result<ClassicGroupFetchTransferTurn, ClassicGroupFetchTransferError> {
    if !position.has_ready_bootstrap_terminal() {
        return Ok(ClassicGroupFetchTransferTurn::Idle);
    }
    let current_fence = current_consumer_group_position_fence(consumer, catalog)
        .map_err(ClassicGroupFetchTransferError::CurrentFence)?;
    transfer_completed_with_fence(position, fetch, current_fence)
}

fn transfer_completed_with_fence(
    position: &mut ClassicGroupPositionExecution,
    fetch: &mut ClassicGroupFetchOwner,
    current_fence: GroupPositionFence,
) -> Result<ClassicGroupFetchTransferTurn, ClassicGroupFetchTransferError> {
    let state = position.replace(ClassicGroupPositionExecutionState::Dormant);
    let ClassicGroupPositionExecutionState::Complete(completed) = state else {
        position.set(state);
        return Ok(ClassicGroupFetchTransferTurn::Idle);
    };
    match fetch.try_activate(completed, current_fence) {
        Ok(()) => Ok(ClassicGroupFetchTransferTurn::Activated),
        Err(ClassicGroupFetchActivationError::Returned(failure)) => {
            let kind = failure.kind();
            let (completed, _copied_input) = failure.into_parts();
            position.set(ClassicGroupPositionExecutionState::Complete(completed));
            Err(ClassicGroupFetchTransferError::Returned(kind))
        }
        Err(ClassicGroupFetchActivationError::Retained(kind)) => {
            Err(ClassicGroupFetchTransferError::Retained(kind))
        }
    }
}

pub(in crate::consumer::group) fn current_position_fence(
    classic: &ClassicGroupOwner,
    catalog: &GroupSessionCatalog,
) -> Result<GroupPositionFence, ClassicGroupFetchCurrentFenceError> {
    let cycle = classic
        .machine()
        .active_cycle()
        .ok_or(ClassicGroupFetchCurrentFenceError::MissingMembershipCycle)?;
    let classic_assignment = classic
        .machine()
        .live_assignment()
        .ok_or(ClassicGroupFetchCurrentFenceError::MissingClassicAssignment)?;
    let catalog_assignment = catalog
        .live_assignment()
        .ok_or(ClassicGroupFetchCurrentFenceError::MissingCatalogAssignment)?;
    if catalog.group_id() != classic.machine().group_id()
        || catalog_assignment.group_id() != catalog.group_id()
    {
        return Err(ClassicGroupFetchCurrentFenceError::CatalogGroupMismatch);
    }
    if classic_assignment != catalog_assignment {
        return Err(ClassicGroupFetchCurrentFenceError::AssignmentMismatch);
    }
    Ok(GroupPositionFence::new(
        catalog_assignment.group_id(),
        cycle,
        catalog_assignment.member_id(),
        catalog_assignment.assignment_generation(),
    ))
}

pub(in crate::consumer::group) fn current_consumer_group_position_fence(
    consumer: &ConsumerGroupExecution,
    catalog: &GroupSessionCatalog,
) -> Result<GroupPositionFence, ClassicGroupFetchCurrentFenceError> {
    let cycle = consumer
        .cycle()
        .ok_or(ClassicGroupFetchCurrentFenceError::MissingMembershipCycle)?;
    let core_assignment = consumer
        .machine()
        .live_assignment()
        .ok_or(ClassicGroupFetchCurrentFenceError::MissingClassicAssignment)?;
    let catalog_assignment = catalog
        .live_assignment()
        .ok_or(ClassicGroupFetchCurrentFenceError::MissingCatalogAssignment)?;
    if catalog_assignment.group_id() != catalog.group_id()
        || core_assignment.group_id() != catalog.group_id()
    {
        return Err(ClassicGroupFetchCurrentFenceError::CatalogGroupMismatch);
    }
    if core_assignment != catalog_assignment {
        return Err(ClassicGroupFetchCurrentFenceError::AssignmentMismatch);
    }
    Ok(GroupPositionFence::new(
        catalog_assignment.group_id(),
        cycle,
        catalog_assignment.member_id(),
        catalog_assignment.assignment_generation(),
    ))
}
