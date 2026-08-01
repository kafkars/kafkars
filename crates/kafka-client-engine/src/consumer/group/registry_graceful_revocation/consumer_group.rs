//! KIP-848 deadline capture, bounded event staging, and terminal assignment transfer.

use kafka_client_core::{
    ClassicGracefulRevocationLease, ClassicGracefulRevocationTerminal, Deadline,
    LiveGroupAssignment, Moment,
};

use super::super::{
    classic_group_graceful_revocation::ClassicGroupRevocationHostError,
    consumer_group_assignment_install::PreparedConsumerGroupAssignmentInstall,
    consumer_group_assignment_retirement::{
        stage_consumer_group_revocation, stage_consumer_group_revocation_owned,
    },
    consumer_group_execution::ConsumerGroupExecutionError,
    registry_entry::GroupConsumerEntry,
};

const TICKS_PER_MILLISECOND: u64 = 1_000_000;

pub(in crate::consumer::group) fn prepare_reconciliation_revocation_deadline(
    entry: &GroupConsumerEntry,
    now: Moment,
    replaces_live_assignment: bool,
) -> Result<Option<Deadline>, ConsumerGroupExecutionError> {
    if !replaces_live_assignment
        || entry.consumer_reconciliation.is_some()
        || entry.fetch.activation().is_none()
    {
        return Ok(None);
    }
    let rebalance_timeout_ticks = u64::from(
        entry
            .consumer
            .as_ref()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .rebalance_timeout_ms(),
    )
    .checked_mul(TICKS_PER_MILLISECOND)
    .ok_or(ConsumerGroupExecutionError::EffectShape)?;
    now.checked_deadline_after(rebalance_timeout_ticks)
        .map(Some)
        .ok_or(ConsumerGroupExecutionError::EffectShape)
}

pub(super) fn stage_consumer_group_graceful_revocation(
    entry: &mut GroupConsumerEntry,
    assignment: LiveGroupAssignment,
    deadline: Deadline,
    now: Moment,
) -> Result<(), ConsumerGroupExecutionError> {
    if entry.catalog.live_assignment() != Some(&assignment) {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    let activation = entry
        .fetch
        .activation()
        .ok_or(ConsumerGroupExecutionError::EffectShape)?;
    let assignment_epoch = activation.binding().assignment_epoch();
    if entry.fetch.machine_assignment_epoch() != Some(assignment_epoch)
        || assignment_epoch.get() != assignment.assignment_generation().get()
    {
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    let named = entry.catalog.prepare_graceful_revocation_event(&assignment);
    let publishes_event = named.is_some();
    let lease = ClassicGracefulRevocationLease::new(assignment_epoch, deadline);
    entry
        .revocation
        .begin_consumer(assignment, lease, now)
        .map_err(|(_error, _assignment)| ConsumerGroupExecutionError::EffectShape)?;
    entry
        .catalog
        .commit_graceful_revocation_event(named, assignment_epoch.get());
    if !publishes_event {
        entry
            .revocation
            .lose_owner()
            .map_err(|_error| ConsumerGroupExecutionError::EffectShape)?;
    }
    Ok(())
}

pub(in crate::consumer::group) fn stage_consumer_group_reconciliation(
    entry: &mut GroupConsumerEntry,
    previous: LiveGroupAssignment,
    install: PreparedConsumerGroupAssignmentInstall,
    revocation_deadline: Option<Deadline>,
    now: Moment,
) -> Result<(), ConsumerGroupExecutionError> {
    entry
        .catalog
        .commit_consumer_group_reconciliation_epoch(install.candidate(), install.member_epoch());
    match revocation_deadline {
        Some(deadline) => stage_consumer_group_graceful_revocation(entry, previous, deadline, now)?,
        None => stage_consumer_group_revocation(entry, Some(previous))?,
    }
    entry.consumer_reconciliation = Some(install);
    Ok(())
}

pub(super) fn settle_consumer_group_revocation(
    entry: &mut GroupConsumerEntry,
) -> Result<bool, ClassicGroupRevocationHostError> {
    let Some(terminal) = entry.revocation.terminal() else {
        return Ok(false);
    };
    let assignment = entry
        .revocation
        .take_pending_consumer()
        .ok_or(ClassicGroupRevocationHostError::MissingPending)?;
    let assignment_epoch = terminal.lease().assignment_epoch();
    let publishes_loss = matches!(terminal, ClassicGracefulRevocationTerminal::Lost { .. });
    if let Err((error, assignment)) = stage_consumer_group_revocation_owned(entry, assignment) {
        entry.revocation.restore_pending_consumer(assignment);
        return Err(ClassicGroupRevocationHostError::ConsumerGroup(error));
    }
    if publishes_loss {
        entry
            .catalog
            .lose_consumer_group_graceful_revocation(assignment_epoch.get());
    }
    entry.revocation.release_terminal(assignment_epoch)?;
    Ok(true)
}
