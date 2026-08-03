//! One bounded Join request submission from retained membership intent.

use std::sync::Arc;

use crate::{
    driver::{
        DriverOwner,
        classic_group::{JoinGroupCallKey, JoinGroupCallReservationError},
    },
    protocol::consumer::{ClassicSyncTopic, classic_join_group_request_with_instance},
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
    let member = match prepared.member_id() {
        Some(member_id) => Some(
            entry
                .catalog
                .required_join_member_spelling(prepared.cycle(), member_id)
                .or_else(|| match entry.catalog.current_member_id() {
                    Some(current) if current == member_id => entry.catalog.current_member(),
                    Some(_) | None => None,
                })
                .map(Arc::as_ref)
                .ok_or(ClassicGroupExecutionError::JoinRequest)?,
        ),
        None => entry.catalog.current_member().map(Arc::as_ref),
    };
    let (owned_partitions, generation) = match prepared.protocol() {
        kafka_client_core::ClassicProtocol::Range => (&[][..], None),
        kafka_client_core::ClassicProtocol::CooperativeSticky => (
            entry
                .classic
                .machine()
                .live_assignment()
                .map_or(&[][..], kafka_client_core::LiveGroupAssignment::partitions),
            entry.classic.machine().live_generation(),
        ),
    };
    let mut owned_topics = Vec::new();
    owned_topics
        .try_reserve_exact(owned_partitions.len())
        .map_err(|_error| ClassicGroupExecutionError::JoinRequest)?;
    let mut prior_topic = None;
    for partition in owned_partitions {
        if prior_topic == Some(partition.topic_id()) {
            continue;
        }
        let topic = entry
            .catalog
            .topic_name(partition.topic_id())
            .map_err(|_error| ClassicGroupExecutionError::JoinRequest)?;
        owned_topics.push(ClassicSyncTopic::new(
            partition.topic_id(),
            Arc::clone(topic),
        ));
        prior_topic = Some(partition.topic_id());
    }
    classic_join_group_request_with_instance(
        entry.catalog.group(),
        member,
        entry.catalog.group_instance_id().map(Arc::as_ref),
        prepared.protocol(),
        &topics,
        owned_partitions,
        &owned_topics,
        generation,
        prepared.timing(),
    )
    .map_err(|_error| ClassicGroupExecutionError::JoinRequest)
}
