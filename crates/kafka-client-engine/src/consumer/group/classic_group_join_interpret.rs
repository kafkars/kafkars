//! Normalized Join facts and deterministic follower or deadline transitions.

use kafka_client_core::{ClassicGroupInput, Moment};

use crate::{
    driver::classic_group::JoinGroupTerminal,
    protocol::consumer::{ClassicJoinOutcome, normalize_classic_join_response},
};

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupJoinSuccessor,
    classic_group_owner_follower::ClassicGroupFollowerJoinError,
    classic_group_rejection_fault::ClassicRejectionPostCore,
    classic_group_rejection_install::{exact_broker_error, install_stage_rejection},
    registry_entry::GroupConsumerEntry,
};

#[expect(
    clippy::large_enum_variant,
    reason = "the interpretation returns one exact linear successor without another allocation"
)]
pub(super) enum JoinInterpretation {
    Confirm(ClassicGroupJoinSuccessor),
    DeferLeader,
}

pub(super) enum JoinInterpretationFailure {
    Restore(ClassicGroupExecutionError),
    PostCore(ClassicGroupExecutionError),
    PostCoreRejection(ClassicRejectionPostCore),
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn interpret_join(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    terminal: &JoinGroupTerminal,
) -> Result<JoinInterpretation, JoinInterpretationFailure> {
    let cycle = terminal.key().cycle();
    if terminal.key().deadline().core().is_elapsed_at(now) {
        apply_join_deadline(entry, cycle, now)?;
        return Ok(JoinInterpretation::Confirm(ClassicGroupJoinSuccessor::Idle));
    }
    let Ok(response) = terminal.result() else {
        apply_join_failure(entry, cycle)?;
        return Ok(JoinInterpretation::Confirm(ClassicGroupJoinSuccessor::Idle));
    };
    let version = terminal
        .selected_version()
        .ok_or(restore(ClassicGroupExecutionError::JoinTerminal))?;
    let outcome = normalize_classic_join_response(version, response)
        .map_err(|_error| restore(ClassicGroupExecutionError::JoinTerminal))?;
    let joined = match outcome {
        ClassicJoinOutcome::Rejected(rejection) => {
            apply_join_rejection(entry, cycle, now, rejection)?;
            return Ok(JoinInterpretation::Confirm(ClassicGroupJoinSuccessor::Idle));
        }
        ClassicJoinOutcome::Joined(joined) => joined,
    };
    if !entry.is_active() {
        apply_join_failure(entry, cycle)?;
        return Ok(JoinInterpretation::Confirm(ClassicGroupJoinSuccessor::Idle));
    }
    let (_throttle, generation, member, role) = joined.into_parts();
    if role.into_leader_members().is_some() {
        return Ok(JoinInterpretation::DeferLeader);
    }
    let candidate = entry
        .catalog
        .prepare_follower_cycle(cycle, member)
        .map_err(|_error| restore(ClassicGroupExecutionError::FollowerJoin))?;
    let prepared = entry
        .classic
        .apply_follower_join(
            entry.catalog.group(),
            candidate,
            generation,
            now,
            terminal.key().deadline(),
        )
        .map_err(classify_follower_join_failure)?;
    Ok(JoinInterpretation::Confirm(
        ClassicGroupJoinSuccessor::Sync(prepared),
    ))
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn apply_join_rejection(
    entry: &mut GroupConsumerEntry,
    cycle: kafka_client_core::MembershipCycle,
    now: Moment,
    rejection: crate::protocol::consumer::ClassicBrokerRejection,
) -> Result<(), JoinInterpretationFailure> {
    let error =
        exact_broker_error(rejection).ok_or(restore(ClassicGroupExecutionError::JoinTerminal))?;
    let transition = entry
        .classic
        .apply(ClassicGroupInput::JoinRejected { cycle, now, error })
        .map_err(|error| restore(ClassicGroupExecutionError::Core(error.kind())))?;
    install_stage_rejection(entry, transition).map_err(JoinInterpretationFailure::PostCoreRejection)
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn apply_join_failure(
    entry: &mut GroupConsumerEntry,
    cycle: kafka_client_core::MembershipCycle,
) -> Result<(), JoinInterpretationFailure> {
    apply_terminal(
        entry,
        ClassicGroupInput::JoinFailed { cycle },
        ClassicGroupExecutionError::JoinTerminal,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn apply_join_deadline(
    entry: &mut GroupConsumerEntry,
    cycle: kafka_client_core::MembershipCycle,
    now: Moment,
) -> Result<(), JoinInterpretationFailure> {
    apply_terminal(
        entry,
        ClassicGroupInput::DeadlineElapsed { cycle, now },
        ClassicGroupExecutionError::JoinTerminal,
    )
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn apply_terminal(
    entry: &mut GroupConsumerEntry,
    input: ClassicGroupInput,
    unexpected: ClassicGroupExecutionError,
) -> Result<(), JoinInterpretationFailure> {
    let transition = entry
        .classic
        .apply(input)
        .map_err(|error| restore(ClassicGroupExecutionError::Core(error.kind())))?;
    if transition.into_effects().next().is_some() {
        return Err(post_core(unexpected));
    }
    Ok(())
}

fn classify_follower_join_failure(
    error: ClassicGroupFollowerJoinError,
) -> JoinInterpretationFailure {
    match error {
        ClassicGroupFollowerJoinError::Candidate(_) => {
            restore(ClassicGroupExecutionError::FollowerJoin)
        }
        ClassicGroupFollowerJoinError::Core(_)
        | ClassicGroupFollowerJoinError::UnexpectedSyncEffect
        | ClassicGroupFollowerJoinError::SyncRequest => {
            post_core(ClassicGroupExecutionError::FollowerJoin)
        }
    }
}

const fn restore(error: ClassicGroupExecutionError) -> JoinInterpretationFailure {
    JoinInterpretationFailure::Restore(error)
}

const fn post_core(error: ClassicGroupExecutionError) -> JoinInterpretationFailure {
    JoinInterpretationFailure::PostCore(error)
}
