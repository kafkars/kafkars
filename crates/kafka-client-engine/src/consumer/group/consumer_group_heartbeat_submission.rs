//! Initial KIP-848 heartbeat materialization and tracked driver handoff.

use kafka_client_core::{ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatRequestKind, Moment};

use crate::driver::{
    ConsumerGroupHeartbeatCall, ConsumerGroupHeartbeatSubmitErrorKind, DriverOwner,
};

use super::{
    consumer_group_close::{
        deadline_terminal, fail_consumer_group_leave, position_failure_allows_consumer_group_leave,
    },
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupExecutionError},
    consumer_group_execution_terminal::fail_consumer_group_entry,
    registry::GroupConsumerRegistry,
    registry_entry::{GroupConsumerEntry, GroupConsumerEntryState},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupHeartbeatSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn submit_one_consumer_group_heartbeat(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<ConsumerGroupHeartbeatSubmissionTurn, ConsumerGroupExecutionError> {
        let Some(index) = self
            .entries
            .iter()
            .position(consumer_group_heartbeat_is_ready)
        else {
            return Ok(ConsumerGroupHeartbeatSubmissionTurn::Idle);
        };
        let entry = &self.entries[index];
        let execution = entry
            .consumer
            .as_ref()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        let prepared = execution
            .prepared()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        if prepared.deadline().core().is_elapsed_at(now) {
            if prepared.kind() == ConsumerGroupHeartbeatRequestKind::Leave {
                fail_consumer_group_leave(
                    &mut self.entries[index],
                    ConsumerGroupHeartbeatFailure::DeadlineElapsed,
                    deadline_terminal(),
                )?;
            } else {
                fail_consumer_group_entry(
                    &mut self.entries[index],
                    ConsumerGroupHeartbeatFailure::DeadlineElapsed,
                )?;
            }
            return Ok(ConsumerGroupHeartbeatSubmissionTurn::Progress);
        }
        let Ok(request) = prepare_request(entry) else {
            fail_consumer_group_entry(
                &mut self.entries[index],
                ConsumerGroupHeartbeatFailure::InvalidResponse,
            )?;
            return Ok(ConsumerGroupHeartbeatSubmissionTurn::Progress);
        };
        match ConsumerGroupHeartbeatCall::submit(
            driver,
            entry.catalog.group(),
            request,
            prepared.deadline(),
        ) {
            Ok(call) => {
                self.entries[index]
                    .consumer
                    .as_mut()
                    .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
                    .install_heartbeat_call(call)?;
                Ok(ConsumerGroupHeartbeatSubmissionTurn::Progress)
            }
            Err(error) if error.kind() == ConsumerGroupHeartbeatSubmitErrorKind::Full => {
                Ok(ConsumerGroupHeartbeatSubmissionTurn::Blocked)
            }
            Err(_error) => {
                fail_consumer_group_entry(
                    &mut self.entries[index],
                    ConsumerGroupHeartbeatFailure::Execution,
                )?;
                Ok(ConsumerGroupHeartbeatSubmissionTurn::Progress)
            }
        }
    }
}

pub(super) fn consumer_group_heartbeat_is_ready(entry: &GroupConsumerEntry) -> bool {
    let leave_is_closing = entry.state == GroupConsumerEntryState::Closing
        && entry.consumer.as_ref().is_some_and(|execution| {
            execution
                .prepared()
                .is_some_and(|prepared| prepared.kind() == ConsumerGroupHeartbeatRequestKind::Leave)
        });
    (entry.is_active() || leave_is_closing)
        && (entry.fault.is_none() || position_failure_allows_consumer_group_leave(entry))
        && entry.consumer.as_ref().is_some_and(|execution| {
            consumer_group_execution_is_ready(execution)
                && execution.heartbeat_call().is_none()
                && join_assignment_is_retired(entry, execution)
        })
}

fn join_assignment_is_retired(
    entry: &GroupConsumerEntry,
    execution: &ConsumerGroupExecution,
) -> bool {
    execution.prepared().is_none_or(|prepared| {
        prepared.kind() != ConsumerGroupHeartbeatRequestKind::Join
            || (entry.consumer_revocation.is_none() && entry.catalog.live_assignment().is_none())
    })
}

pub(super) fn consumer_group_execution_is_ready(execution: &ConsumerGroupExecution) -> bool {
    execution.prepared().is_some()
        && execution.machine().retry_schedule().is_none()
        && execution.topic_identity_call().is_none()
        && execution.topic_identities().is_complete()
        && execution.rediscovery_state().permits_submission()
}

pub(super) fn prepare_request(
    entry: &GroupConsumerEntry,
) -> Result<crate::protocol::consumer::PreparedConsumerGroupHeartbeatRequest, ()> {
    let execution = entry.consumer.as_ref().ok_or(())?;
    let prepared = execution.prepared().ok_or(())?;
    match prepared.kind() {
        ConsumerGroupHeartbeatRequestKind::Join => prepare_join_request(entry, execution),
        ConsumerGroupHeartbeatRequestKind::Steady => prepare_steady_request(entry, execution),
        ConsumerGroupHeartbeatRequestKind::Leave => prepare_leave_request(entry, execution),
    }
}

fn prepare_join_request(
    entry: &GroupConsumerEntry,
    execution: &ConsumerGroupExecution,
) -> Result<crate::protocol::consumer::PreparedConsumerGroupHeartbeatRequest, ()> {
    let prepared = execution.prepared().ok_or(())?;
    let member = match prepared.member_id() {
        Some(member_id) if entry.catalog.current_member_id() == Some(member_id) => {
            Some(entry.catalog.current_member().ok_or(())?.as_ref())
        }
        Some(_) => return Err(()),
        None => None,
    };
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(entry.catalog.local_subscription().len())
        .map_err(|_error| ())?;
    for topic_id in entry.catalog.local_subscription() {
        topics.push(
            entry
                .catalog
                .topic_name(*topic_id)
                .map_err(|_error| ())?
                .as_ref(),
        );
    }
    ConsumerGroupHeartbeatCall::join_request(
        entry.catalog.group(),
        member,
        entry
            .catalog
            .group_instance_id()
            .map(std::convert::AsRef::as_ref),
        execution.rebalance_timeout_ms(),
        &topics,
    )
    .map_err(|_error| ())
}

fn prepare_steady_request(
    entry: &GroupConsumerEntry,
    execution: &ConsumerGroupExecution,
) -> Result<crate::protocol::consumer::PreparedConsumerGroupHeartbeatRequest, ()> {
    let prepared = execution.prepared().ok_or(())?;
    let member_id = prepared.member_id().ok_or(())?;
    let member_epoch = prepared.member_epoch().ok_or(())?;
    let reportable = execution.machine().live_assignment();
    match (prepared.assignment_generation(), reportable) {
        (Some(generation), Some(assignment))
            if assignment.member_id() == member_id
                && assignment.assignment_generation() == generation => {}
        (None, None) if execution.machine().pending_assignment().is_some() => {}
        _ => return Err(()),
    }
    if entry.catalog.current_member_id() != Some(member_id)
        || entry.catalog.consumer_group_member_epoch() != Some(member_epoch)
    {
        return Err(());
    }
    let member = entry.catalog.current_member().ok_or(())?;
    let owned = execution
        .topic_identities()
        .owned_topics(reportable.map_or(&[], |assignment| assignment.partitions()))
        .map_err(|_error| ())?;
    ConsumerGroupHeartbeatCall::steady_request(
        entry.catalog.group(),
        member,
        member_epoch.get(),
        Some(&owned),
    )
    .map_err(|_error| ())
}

fn prepare_leave_request(
    entry: &GroupConsumerEntry,
    execution: &ConsumerGroupExecution,
) -> Result<crate::protocol::consumer::PreparedConsumerGroupHeartbeatRequest, ()> {
    let prepared = execution.prepared().ok_or(())?;
    let member_id = prepared.member_id().ok_or(())?;
    let member_epoch = prepared.member_epoch().ok_or(())?;
    let assignment_generation = prepared.assignment_generation().ok_or(())?;
    let assignment = execution.machine().live_assignment().ok_or(())?;
    if assignment.member_id() != member_id
        || assignment.assignment_generation() != assignment_generation
        || entry.catalog.current_member_id() != Some(member_id)
        || entry.catalog.consumer_group_member_epoch() != Some(member_epoch)
    {
        return Err(());
    }
    ConsumerGroupHeartbeatCall::leave_request(
        entry.catalog.group(),
        entry.catalog.current_member().ok_or(())?,
    )
    .map_err(|_error| ())
}
