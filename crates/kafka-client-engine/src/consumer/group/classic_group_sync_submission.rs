//! One bounded follower Sync submission from an opaque prepared request.

use std::sync::Arc;

use kafka_client_core::ClassicGroupInput;

use crate::driver::{
    DriverOwner,
    classic_group::{SyncGroupCallKey, SyncGroupCallReservationError},
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupSyncSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn submit_one_classic_sync(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ClassicGroupSyncSubmissionTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(sync_is_ready) else {
            return Ok(ClassicGroupSyncSubmissionTurn::Idle);
        };
        let entry = &self.entries[index];
        let prepared = entry
            .execution
            .prepared_sync()
            .ok_or(ClassicGroupExecutionError::SyncNotPrepared)?;
        let identity = prepared.identity();
        let key = SyncGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
        let group = Arc::clone(entry.catalog.group());
        let calls = self
            .sync_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
        let permit = match calls.try_reserve_sync_group(key, &group) {
            Ok(permit) => permit,
            Err(SyncGroupCallReservationError::Capacity { .. }) => {
                return Ok(ClassicGroupSyncSubmissionTurn::Blocked);
            }
            Err(SyncGroupCallReservationError::Duplicate { .. }) => {
                return Err(ClassicGroupExecutionError::CallIdentityMismatch);
            }
        };
        let entry = &mut self.entries[index];
        let prepared = entry.execution.begin_sync_handoff()?;
        let (identity, request) = prepared.into_parts();
        match permit.submit(driver, request) {
            Ok(accepted) => {
                match entry
                    .execution
                    .confirm_sync_driver_owned(identity, accepted)
                {
                    Ok(()) => Ok(ClassicGroupSyncSubmissionTurn::Progress),
                    Err(failure) => {
                        entry.fault = Some(ClassicGroupEntryFault::SyncAcceptance(failure));
                        Err(ClassicGroupExecutionError::HandoffMismatch)
                    }
                }
            }
            Err(failure) => {
                let transition = match entry.classic.apply(ClassicGroupInput::SyncFailed {
                    cycle: identity.cycle(),
                }) {
                    Ok(transition) => transition,
                    Err(error) => {
                        entry.fault = Some(ClassicGroupEntryFault::SyncSubmission(failure));
                        return Err(ClassicGroupExecutionError::Core(error.kind()));
                    }
                };
                if transition.into_effects().next().is_some() {
                    entry.fault = Some(ClassicGroupEntryFault::SyncSubmission(failure));
                    return Err(ClassicGroupExecutionError::SyncTerminal);
                }
                if let Err(error) = entry.execution.finish_sync_submission_failure(identity) {
                    entry.fault = Some(ClassicGroupEntryFault::SyncSubmission(failure));
                    return Err(error);
                }
                drop(failure);
                Ok(ClassicGroupSyncSubmissionTurn::Progress)
            }
        }
    }
}

fn sync_is_ready(entry: &GroupConsumerEntry) -> bool {
    entry.is_active() && entry.execution.prepared_sync().is_some()
}
