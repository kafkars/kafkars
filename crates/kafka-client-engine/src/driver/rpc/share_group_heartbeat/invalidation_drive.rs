//! Fair submission and terminal polling for share membership route invalidation.
#![allow(
    dead_code,
    reason = "the share invalidation driver turn precedes its hosted registry checkpoint"
)]

use kafka_driver::{CompletionError, InvalidationDisposition};

use crate::driver::DriverOwner;

use super::invalidation::{
    PendingShareCoordinatorInvalidation, ShareCoordinatorInvalidationAdmissionFailure,
    ShareCoordinatorInvalidationPermission, ShareCoordinatorInvalidationPoll,
    ShareCoordinatorInvalidationState, ShareCoordinatorInvalidationTerminalFailure,
    ShareCoordinatorInvalidations,
};

impl ShareCoordinatorInvalidations {
    pub(crate) fn drive_one(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ShareCoordinatorInvalidationPoll, ShareCoordinatorInvalidationAdmissionFailure>
    {
        if let Some((index, result)) = ready_terminal(&self.entries) {
            let state = self.entries.remove(index);
            let group_id = state.group_id();
            drop(state);
            return Ok(terminal(group_id, result));
        }
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| matches!(entry, ShareCoordinatorInvalidationState::Queued(_)))
        {
            let ShareCoordinatorInvalidationState::Queued(pending) = self.entries.remove(index)
            else {
                unreachable!("selected queued share invalidation")
            };
            let (group_id, token) = pending.into_parts();
            match driver.driver.invalidate(token) {
                Ok(call) => {
                    self.entries
                        .push(ShareCoordinatorInvalidationState::Active { group_id, call });
                    return Ok(ShareCoordinatorInvalidationPoll::Submitted { group_id });
                }
                Err(rejected) => {
                    let (source, token) = rejected.into_parts();
                    self.entries.push(ShareCoordinatorInvalidationState::Queued(
                        PendingShareCoordinatorInvalidation::new(group_id, token),
                    ));
                    return Err(ShareCoordinatorInvalidationAdmissionFailure::new(
                        group_id, source,
                    ));
                }
            }
        }
        Ok(self
            .entries
            .first()
            .map_or(ShareCoordinatorInvalidationPoll::Idle, |entry| {
                ShareCoordinatorInvalidationPoll::Pending {
                    group_id: entry.group_id(),
                }
            }))
    }
}

fn ready_terminal(
    entries: &[ShareCoordinatorInvalidationState],
) -> Option<(usize, Result<InvalidationDisposition, CompletionError>)> {
    entries
        .iter()
        .enumerate()
        .find_map(|(index, entry)| match entry {
            ShareCoordinatorInvalidationState::Active { call, .. } => {
                call.try_result().map(|result| (index, result))
            }
            ShareCoordinatorInvalidationState::Queued(_) => None,
        })
}

pub(super) const fn terminal(
    group_id: kafka_client_core::GroupId,
    result: Result<InvalidationDisposition, CompletionError>,
) -> ShareCoordinatorInvalidationPoll {
    let result = match result {
        Ok(InvalidationDisposition::Applied) => Ok(ShareCoordinatorInvalidationPermission::Applied),
        Ok(InvalidationDisposition::IgnoredStale) => {
            Ok(ShareCoordinatorInvalidationPermission::IgnoredStale)
        }
        Ok(InvalidationDisposition::Unavailable) => {
            Ok(ShareCoordinatorInvalidationPermission::Unavailable)
        }
        Ok(InvalidationDisposition::CapacityReached) => {
            Err(ShareCoordinatorInvalidationTerminalFailure::CapacityReached)
        }
        Ok(_) => Err(ShareCoordinatorInvalidationTerminalFailure::UnrecognizedDisposition),
        Err(source) => Err(ShareCoordinatorInvalidationTerminalFailure::Completion(
            source,
        )),
    };
    ShareCoordinatorInvalidationPoll::Terminal { group_id, result }
}
