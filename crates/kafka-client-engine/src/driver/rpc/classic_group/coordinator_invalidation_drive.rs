//! Fair bounded submission and terminal polling for coordinator invalidation.

use kafka_driver::{
    Call, CompletionError, InvalidationDisposition, InvalidationSubmitError, RouteFailureToken,
};

use crate::driver::DriverOwner;

use super::{
    coordinator_invalidation::{
        ClassicCoordinatorInvalidationPermission, ClassicCoordinatorInvalidationPoll,
        ClassicCoordinatorInvalidationState, ClassicCoordinatorInvalidationTerminal,
        ClassicCoordinatorInvalidationTerminalFailure, ClassicCoordinatorInvalidations,
        PendingClassicCoordinatorInvalidation,
    },
    coordinator_invalidation_admission::ClassicCoordinatorInvalidationAdmissionFailure,
};

impl DriverOwner {
    fn submit_classic_coordinator_invalidation(
        &self,
        route_token: RouteFailureToken,
    ) -> Result<Call<InvalidationDisposition>, InvalidationSubmitError> {
        self.driver.invalidate(route_token)
    }
}

impl ClassicCoordinatorInvalidations {
    pub(crate) fn drive_one(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ClassicCoordinatorInvalidationPoll, ClassicCoordinatorInvalidationAdmissionFailure>
    {
        if let Some((index, result)) = ready_terminal(&self.entries) {
            let state = self.entries.remove(index);
            let group_id = state.group_id();
            drop(state);
            if let Err(source) = result {
                self.entries
                    .push(ClassicCoordinatorInvalidationState::CompletionFailed {
                        group_id,
                        source,
                    });
            }
            return Ok(ClassicCoordinatorInvalidationPoll::Terminal(terminal(
                group_id, result,
            )));
        }

        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| matches!(entry, ClassicCoordinatorInvalidationState::Queued(_)))
        {
            let state = self.entries.remove(index);
            let ClassicCoordinatorInvalidationState::Queued(pending) = state else {
                return Ok(ClassicCoordinatorInvalidationPoll::Idle);
            };
            let (group_id, route_token) = pending.into_parts();
            match driver.submit_classic_coordinator_invalidation(route_token) {
                Ok(call) => {
                    self.entries
                        .push(ClassicCoordinatorInvalidationState::Active { group_id, call });
                    return Ok(ClassicCoordinatorInvalidationPoll::Submitted { group_id });
                }
                Err(rejected) => {
                    let (source, route_token) = rejected.into_parts();
                    self.entries
                        .push(ClassicCoordinatorInvalidationState::Queued(
                            PendingClassicCoordinatorInvalidation::new(group_id, route_token),
                        ));
                    return Err(ClassicCoordinatorInvalidationAdmissionFailure::new(
                        group_id, source,
                    ));
                }
            }
        }

        if let Some((group_id, source)) = self.entries.iter().find_map(|entry| match entry {
            ClassicCoordinatorInvalidationState::CompletionFailed { group_id, source } => {
                Some((*group_id, *source))
            }
            ClassicCoordinatorInvalidationState::Queued(_)
            | ClassicCoordinatorInvalidationState::Active { .. } => None,
        }) {
            return Ok(ClassicCoordinatorInvalidationPoll::Terminal(terminal(
                group_id,
                Err(source),
            )));
        }

        Ok(self
            .entries
            .first()
            .map_or(ClassicCoordinatorInvalidationPoll::Idle, |entry| {
                ClassicCoordinatorInvalidationPoll::Pending {
                    group_id: entry.group_id(),
                }
            }))
    }
}

fn ready_terminal(
    entries: &[ClassicCoordinatorInvalidationState],
) -> Option<(usize, Result<InvalidationDisposition, CompletionError>)> {
    entries
        .iter()
        .enumerate()
        .find_map(|(index, entry)| match entry {
            ClassicCoordinatorInvalidationState::Active { call, .. } => {
                call.try_result().map(|result| (index, result))
            }
            ClassicCoordinatorInvalidationState::Queued(_)
            | ClassicCoordinatorInvalidationState::CompletionFailed { .. } => None,
        })
}

pub(super) fn terminal(
    group_id: kafka_client_core::GroupId,
    result: Result<InvalidationDisposition, CompletionError>,
) -> ClassicCoordinatorInvalidationTerminal {
    let result = match result {
        Ok(InvalidationDisposition::Applied) => {
            Ok(ClassicCoordinatorInvalidationPermission::Applied)
        }
        Ok(InvalidationDisposition::IgnoredStale) => {
            Ok(ClassicCoordinatorInvalidationPermission::IgnoredStale)
        }
        Ok(InvalidationDisposition::Unavailable) => {
            Err(ClassicCoordinatorInvalidationTerminalFailure::Unavailable)
        }
        Ok(InvalidationDisposition::CapacityReached) => {
            Err(ClassicCoordinatorInvalidationTerminalFailure::CapacityReached)
        }
        Ok(_) => Err(ClassicCoordinatorInvalidationTerminalFailure::UnrecognizedDisposition),
        Err(source) => Err(ClassicCoordinatorInvalidationTerminalFailure::Completion(
            source,
        )),
    };
    ClassicCoordinatorInvalidationTerminal::new(group_id, result)
}
