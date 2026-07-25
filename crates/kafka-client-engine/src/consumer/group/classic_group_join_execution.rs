//! One bounded Join request submission from retained membership intent.

use std::sync::Arc;

use crate::{
    driver::{
        DriverOwner,
        classic_group::{JoinGroupCallKey, JoinGroupCallReservationError},
    },
    protocol::consumer::classic_join_group_request,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::PreparedClassicGroupJoin, registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupJoinSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn submit_one_classic_join(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ClassicGroupJoinSubmissionTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(join_is_ready) else {
            return Ok(ClassicGroupJoinSubmissionTurn::Idle);
        };
        let entry = &self.entries[index];
        let prepared = entry
            .execution
            .prepared_join()
            .ok_or(ClassicGroupExecutionError::JoinNotPrepared)?;
        let request = prepare_join_request(entry, prepared)?;
        let key = JoinGroupCallKey::new(prepared.group_id(), prepared.cycle(), prepared.deadline());
        let group = Arc::clone(entry.catalog.group());
        let calls = self
            .join_calls
            .as_mut()
            .ok_or(ClassicGroupExecutionError::CallRegistryUnavailable)?;
        let permit = match calls.try_reserve_join_group(key, &group) {
            Ok(permit) => permit,
            Err(JoinGroupCallReservationError::Capacity { .. }) => {
                return Ok(ClassicGroupJoinSubmissionTurn::Blocked);
            }
            Err(JoinGroupCallReservationError::Duplicate { .. }) => {
                return Err(ClassicGroupExecutionError::CallIdentityMismatch);
            }
        };
        let entry = &mut self.entries[index];
        let handoff = entry.execution.begin_join_handoff()?;
        match permit.submit(driver, request) {
            Ok(accepted) => {
                match entry
                    .execution
                    .confirm_join_driver_owned(handoff.into_driver_acceptance(), accepted)
                {
                    Ok(()) => Ok(ClassicGroupJoinSubmissionTurn::Progress),
                    Err(failure) => {
                        entry.fault = Some(ClassicGroupEntryFault::JoinAcceptance(failure));
                        Err(ClassicGroupExecutionError::HandoffMismatch)
                    }
                }
            }
            Err(_failure) => match entry.execution.restore_join(handoff) {
                Ok(()) => Ok(ClassicGroupJoinSubmissionTurn::Blocked),
                Err((error, _handoff)) => Err(error),
            },
        }
    }
}

fn join_is_ready(entry: &GroupConsumerEntry) -> bool {
    entry.is_active() && entry.execution.prepared_join().is_some()
}

fn prepare_join_request(
    entry: &GroupConsumerEntry,
    prepared: &PreparedClassicGroupJoin,
) -> Result<crate::protocol::consumer::PreparedClassicJoinGroupRequest, ClassicGroupExecutionError>
{
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(entry.catalog.local_subscription().len())
        .map_err(|_error| ClassicGroupExecutionError::JoinRequest)?;
    for topic_id in entry.catalog.local_subscription() {
        let topic = entry
            .catalog
            .topic_name(*topic_id)
            .map_err(|_error| ClassicGroupExecutionError::JoinRequest)?;
        topics.push(Arc::clone(topic));
    }
    classic_join_group_request(
        entry.catalog.group(),
        entry.catalog.current_member().map(Arc::as_ref),
        &topics,
        prepared.timing(),
    )
    .map_err(|_error| ClassicGroupExecutionError::JoinRequest)
}
