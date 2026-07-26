//! Exact membership-confirmation transfer into bounded coordinator invalidation.

use crate::driver::classic_group::{
    ClassicCoordinatorInvalidationPermit, PendingClassicCoordinatorInvalidation,
    TrackedClassicHeartbeatCalls, TrackedJoinGroupCalls, TrackedSyncGroupCalls,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat::{
        ClassicHeartbeatDriverOwner, ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor,
    },
    classic_group_join::{ClassicGroupExecutionState, ClassicGroupJoinSuccessor},
    classic_group_join_call::ClassicGroupJoinCallOwner,
    classic_group_sync::ClassicGroupSyncDriverOwner,
    registry_entry::GroupConsumerEntry,
};

pub(super) fn confirm_join_rediscovery(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedJoinGroupCalls,
    permit: ClassicCoordinatorInvalidationPermit<'_>,
) -> Result<(), ClassicGroupExecutionError> {
    if !entry.rediscovery.awaits_route_transfer() {
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationGate);
    }
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let ClassicGroupExecutionState::JoinConfirmationPending { call, successor } = state else {
        entry.execution.set_execution_state(state);
        return Err(ClassicGroupExecutionError::HandoffMismatch);
    };
    if !matches!(&successor, ClassicGroupJoinSuccessor::Idle)
        || call.accepted().key().group_id() != permit.group_id()
    {
        entry
            .execution
            .set_execution_state(ClassicGroupExecutionState::JoinConfirmationPending {
                call,
                successor,
            });
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer);
    }
    let (integration, tracking, accepted) = call.into_parts();
    match calls.extract_join_group_rediscovery(accepted) {
        Ok(pending) => commit_transfer(entry, permit, pending),
        Err(failure) => {
            let (accepted, _error) = failure.into_parts();
            entry.execution.set_execution_state(
                ClassicGroupExecutionState::JoinConfirmationPending {
                    call: ClassicGroupJoinCallOwner::new(integration, tracking, accepted),
                    successor,
                },
            );
            Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer)
        }
    }
}

pub(super) fn confirm_sync_rediscovery(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedSyncGroupCalls,
    permit: ClassicCoordinatorInvalidationPermit<'_>,
) -> Result<(), ClassicGroupExecutionError> {
    if !entry.rediscovery.awaits_route_transfer() {
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationGate);
    }
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let ClassicGroupExecutionState::SyncConfirmationPending(owner) = state else {
        entry.execution.set_execution_state(state);
        return Err(ClassicGroupExecutionError::HandoffMismatch);
    };
    let (identity, accepted) = owner.into_parts();
    if accepted.key().group_id() != permit.group_id() {
        entry
            .execution
            .set_execution_state(ClassicGroupExecutionState::SyncConfirmationPending(
                ClassicGroupSyncDriverOwner::new(identity, accepted),
            ));
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer);
    }
    match calls.extract_sync_group_rediscovery(accepted) {
        Ok(pending) => commit_transfer(entry, permit, pending),
        Err(failure) => {
            let (accepted, _error) = failure.into_parts();
            entry.execution.set_execution_state(
                ClassicGroupExecutionState::SyncConfirmationPending(
                    ClassicGroupSyncDriverOwner::new(identity, accepted),
                ),
            );
            Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer)
        }
    }
}

pub(super) fn confirm_heartbeat_rediscovery(
    entry: &mut GroupConsumerEntry,
    calls: &mut TrackedClassicHeartbeatCalls,
    permit: ClassicCoordinatorInvalidationPermit<'_>,
) -> Result<(), ClassicGroupExecutionError> {
    if !entry.rediscovery.awaits_route_transfer() {
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationGate);
    }
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let ClassicHeartbeatExecutionState::ConfirmationPending { owner, successor } = state else {
        entry.heartbeat.set(state);
        return Err(ClassicGroupExecutionError::HeartbeatState);
    };
    if !matches!(&successor, ClassicHeartbeatSuccessor::Dormant)
        || owner.accepted().key().group_id() != permit.group_id()
    {
        entry
            .heartbeat
            .set(ClassicHeartbeatExecutionState::ConfirmationPending { owner, successor });
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer);
    }
    match calls.extract_classic_heartbeat_rediscovery(owner.into_accepted()) {
        Ok(pending) => commit_transfer(entry, permit, pending),
        Err(failure) => {
            let (accepted, _error) = failure.into_parts();
            entry
                .heartbeat
                .set(ClassicHeartbeatExecutionState::ConfirmationPending {
                    owner: ClassicHeartbeatDriverOwner::new(accepted),
                    successor,
                });
            Err(ClassicGroupExecutionError::CoordinatorInvalidationTransfer)
        }
    }
}

fn commit_transfer(
    entry: &mut GroupConsumerEntry,
    permit: ClassicCoordinatorInvalidationPermit<'_>,
    pending: PendingClassicCoordinatorInvalidation,
) -> Result<(), ClassicGroupExecutionError> {
    if let Err(failure) = permit.install(pending) {
        entry.fault = Some(ClassicGroupEntryFault::CoordinatorInvalidationInstall(
            failure,
        ));
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationInstall);
    }
    if entry.rediscovery.confirm_rediscovery_transfer().is_err() {
        entry.fault = Some(ClassicGroupEntryFault::CoordinatorInvalidationGate);
        return Err(ClassicGroupExecutionError::CoordinatorInvalidationGate);
    }
    Ok(())
}
