//! Conservative Heartbeat reconciliation after the unique driver owner is gone.

use kafka_client_core::{ClassicGroupEffect, ClassicGroupInput};

use crate::driver::classic_group::{
    AcceptedClassicHeartbeatCall, RecoveredClassicHeartbeatOwnership,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat::{
        ClassicHeartbeatDriverOwner, ClassicHeartbeatExecutionState, ClassicHeartbeatSuccessor,
    },
    classic_group_heartbeat_prepare::{commit_revoke, map_revocation_kind},
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

impl GroupConsumerRegistry {
    pub(super) fn recover_classic_heartbeats_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ClassicGroupExecutionError> {
        if self.heartbeat_recovery_fault.is_some() {
            return Err(ClassicGroupExecutionError::EntryFault);
        }
        if self.heartbeat_shutdown_recovery.is_none() {
            let calls = self
                .heartbeat_calls
                .take()
                .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
            self.heartbeat_shutdown_recovery =
                Some(calls.recover_classic_heartbeats_after_driver_shutdown());
        }
        while let Some(recovered) = self.take_next_heartbeat_recovery() {
            if let Err((error, recovered)) = self.reconcile_heartbeat_recovery(recovered) {
                self.heartbeat_recovery_fault = Some(recovered);
                return Err(error);
            }
        }
        self.heartbeat_shutdown_recovery = None;
        Ok(())
    }

    fn take_next_heartbeat_recovery(&mut self) -> Option<RecoveredClassicHeartbeatOwnership> {
        let recovery = self.heartbeat_shutdown_recovery.as_mut()?;
        recovery
            .pop_active()
            .or_else(|| recovery.take_settled())
            .or_else(|| recovery.take_pending())
            .or_else(|| recovery.take_completion())
    }

    #[expect(
        clippy::result_large_err,
        reason = "failed recovery returns the exact linear generated Heartbeat owner"
    )]
    fn reconcile_heartbeat_recovery(
        &mut self,
        recovered: RecoveredClassicHeartbeatOwnership,
    ) -> Result<
        (),
        (
            ClassicGroupExecutionError,
            RecoveredClassicHeartbeatOwnership,
        ),
    > {
        let key = recovered.key();
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == key.group_id())
        else {
            return Err((ClassicGroupExecutionError::CallIdentityMismatch, recovered));
        };
        if entry
            .heartbeat
            .accepted()
            .map(AcceptedClassicHeartbeatCall::key)
            != Some(key)
        {
            return Err((ClassicGroupExecutionError::CallIdentityMismatch, recovered));
        }
        let before_semantic = matches!(
            entry.heartbeat.state(),
            ClassicHeartbeatExecutionState::DriverOwned(_)
        );
        if before_semantic && entry.fault.is_none() {
            if let Err(error) = apply_recovery_loss(entry, key.attempt()) {
                return Err((error, recovered));
            }
        }
        reconcile_exact_recovery(entry, recovered, before_semantic)
    }
}

fn apply_recovery_loss(
    entry: &mut GroupConsumerEntry,
    attempt: kafka_client_core::ClassicHeartbeatAttempt,
) -> Result<(), ClassicGroupExecutionError> {
    let transition = match entry
        .classic
        .apply(ClassicGroupInput::HeartbeatFailed { attempt })
    {
        Ok(transition) => transition,
        Err(error) => {
            entry.fault = Some(ClassicGroupEntryFault::HeartbeatRecoverySemantic(attempt));
            return Err(ClassicGroupExecutionError::Core(error.kind()));
        }
    };
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = effects.next()
    else {
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatRecoverySemantic(attempt));
        return Err(ClassicGroupExecutionError::HeartbeatTerminal);
    };
    if effects.next().is_some() {
        entry.fault = Some(ClassicGroupEntryFault::HeartbeatRecoverySemantic(attempt));
        return Err(ClassicGroupExecutionError::HeartbeatTerminal);
    }
    match commit_revoke(entry, assignment, classic_generation) {
        Ok(()) => Ok(()),
        Err(failure) => {
            let kind = failure.kind;
            entry.fault = Some(ClassicGroupEntryFault::HeartbeatLocalRevoke { failure });
            Err(map_revocation_kind(kind))
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "failed reconciliation returns the exact generated Heartbeat owner"
)]
fn reconcile_exact_recovery(
    entry: &mut GroupConsumerEntry,
    recovered: RecoveredClassicHeartbeatOwnership,
    before_semantic: bool,
) -> Result<
    (),
    (
        ClassicGroupExecutionError,
        RecoveredClassicHeartbeatOwnership,
    ),
> {
    let state = entry
        .heartbeat
        .replace(ClassicHeartbeatExecutionState::Dormant);
    let (owner, successor) = match state {
        ClassicHeartbeatExecutionState::DriverOwned(owner) if before_semantic => {
            (owner, ClassicHeartbeatSuccessor::Dormant)
        }
        ClassicHeartbeatExecutionState::ConfirmationPending { owner, successor }
            if !before_semantic =>
        {
            (owner, successor)
        }
        state => {
            entry.heartbeat.set(state);
            return Err((ClassicGroupExecutionError::HeartbeatState, recovered));
        }
    };
    match recovered.reconcile_classic_heartbeat_after_driver_shutdown(owner.into_accepted()) {
        Ok(()) => {
            entry.heartbeat.set(successor.into_state());
            Ok(())
        }
        Err(failure) => {
            let (accepted, recovered, _error) = failure.into_parts();
            restore_recovery_state(entry, accepted, successor, before_semantic);
            Err((ClassicGroupExecutionError::CallIdentityMismatch, recovered))
        }
    }
}

fn restore_recovery_state(
    entry: &mut GroupConsumerEntry,
    accepted: crate::driver::classic_group::AcceptedClassicHeartbeatCall,
    successor: ClassicHeartbeatSuccessor,
    before_semantic: bool,
) {
    let owner = ClassicHeartbeatDriverOwner::new(accepted);
    entry.heartbeat.set(if before_semantic {
        ClassicHeartbeatExecutionState::DriverOwned(owner)
    } else {
        ClassicHeartbeatExecutionState::ConfirmationPending { owner, successor }
    });
}
