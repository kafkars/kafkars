//! Generated Heartbeat terminal normalization and deterministic loss policy.

use kafka_client_core::{ClassicGroupEffect, ClassicGroupInput, ClassicGroupTransition, Moment};

use crate::{
    driver::classic_group::ClassicHeartbeatTerminal,
    protocol::consumer::{
        ClassicHeartbeatOutcome, normalize_classic_heartbeat_response, throttle_ticks,
    },
};

use super::{
    classic_group_assignment::ClassicGroupRevocationFailure,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_heartbeat::ClassicHeartbeatSuccessor,
    classic_group_heartbeat_prepare::commit_revoke,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    classic_group_rejection_fault::ClassicRejectionPostCore,
    classic_group_rejection_install::exact_broker_error, registry_entry::GroupConsumerEntry,
};

pub(super) enum ClassicHeartbeatInterpretationFailure {
    Restorable(ClassicGroupExecutionError),
    PostCore(ClassicGroupExecutionError),
    PostCoreRejection(ClassicRejectionPostCore),
    Revoke(ClassicGroupRevocationFailure),
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
pub(super) fn interpret_heartbeat(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    terminal: &ClassicHeartbeatTerminal,
) -> Result<ClassicHeartbeatSuccessor, ClassicHeartbeatInterpretationFailure> {
    let key = terminal.key();
    if key.deadline().core().is_elapsed_at(now) {
        let transition = entry
            .classic
            .apply(ClassicGroupInput::HeartbeatDeadlineElapsed {
                attempt: key.attempt(),
                now,
            })
            .map_err(|error| {
                ClassicHeartbeatInterpretationFailure::Restorable(ClassicGroupExecutionError::Core(
                    error.kind(),
                ))
            })?;
        return commit_terminal_loss(entry, transition);
    }
    let outcome = terminal.result().as_ref().ok().and_then(|response| {
        terminal
            .selected_version()
            .and_then(|version| normalize_classic_heartbeat_response(version, response).ok())
    });
    let transition = match outcome {
        Some(ClassicHeartbeatOutcome::Succeeded { throttle_time_ms }) => {
            let ticks = throttle_ticks(throttle_time_ms).ok_or(
                ClassicHeartbeatInterpretationFailure::Restorable(
                    ClassicGroupExecutionError::HeartbeatTerminal,
                ),
            )?;
            entry
                .classic
                .apply(ClassicGroupInput::HeartbeatSucceeded {
                    attempt: key.attempt(),
                    now,
                    throttle_ticks: ticks,
                })
                .map_err(|error| {
                    ClassicHeartbeatInterpretationFailure::Restorable(
                        ClassicGroupExecutionError::Core(error.kind()),
                    )
                })?
        }
        Some(ClassicHeartbeatOutcome::Rejected(rejection)) => {
            let error = exact_broker_error(rejection).ok_or(
                ClassicHeartbeatInterpretationFailure::Restorable(
                    ClassicGroupExecutionError::HeartbeatTerminal,
                ),
            )?;
            let transition = entry
                .classic
                .apply(ClassicGroupInput::HeartbeatRejected {
                    attempt: key.attempt(),
                    now,
                    error,
                })
                .map_err(|error| {
                    ClassicHeartbeatInterpretationFailure::Restorable(
                        ClassicGroupExecutionError::Core(error.kind()),
                    )
                })?;
            install_heartbeat_rejection(entry, transition)
                .map_err(ClassicHeartbeatInterpretationFailure::PostCoreRejection)?;
            return Ok(ClassicHeartbeatSuccessor::Dormant);
        }
        None => entry
            .classic
            .apply(ClassicGroupInput::HeartbeatFailed {
                attempt: key.attempt(),
            })
            .map_err(|error| {
                ClassicHeartbeatInterpretationFailure::Restorable(ClassicGroupExecutionError::Core(
                    error.kind(),
                ))
            })?,
    };
    interpret_terminal_transition(entry, key.attempt(), transition)
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn interpret_terminal_transition(
    entry: &mut GroupConsumerEntry,
    attempt: kafka_client_core::ClassicHeartbeatAttempt,
    transition: ClassicGroupTransition,
) -> Result<ClassicHeartbeatSuccessor, ClassicHeartbeatInterpretationFailure> {
    let mut effects = transition.into_effects().take(2);
    let first = effects.next();
    let second = effects.next();
    match (first, second) {
        (Some(ClassicGroupEffect::ArmHeartbeat { schedule }), None) => {
            if successor_matches(attempt, schedule) {
                Ok(ClassicHeartbeatSuccessor::Waiting(schedule))
            } else {
                Err(ClassicHeartbeatInterpretationFailure::PostCore(
                    ClassicGroupExecutionError::HeartbeatTerminal,
                ))
            }
        }
        (
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            None,
        ) => match commit_revoke(entry, assignment, classic_generation) {
            Ok(()) => Ok(ClassicHeartbeatSuccessor::Dormant),
            Err(failure) => Err(ClassicHeartbeatInterpretationFailure::Revoke(failure)),
        },
        _ => Err(ClassicHeartbeatInterpretationFailure::PostCore(
            ClassicGroupExecutionError::HeartbeatTerminal,
        )),
    }
}

#[expect(
    clippy::maybe_infinite_iter,
    reason = "this predicate compares fixed scalar heartbeat fences and performs no iteration"
)]
fn successor_matches(
    settled: kafka_client_core::ClassicHeartbeatAttempt,
    successor: kafka_client_core::ClassicHeartbeatSchedule,
) -> bool {
    successor.attempt().cycle() == settled.cycle()
        && successor.attempt().assignment_generation() == settled.assignment_generation()
        && settled
            .sequence()
            .get()
            .checked_add(1)
            .is_some_and(|next| successor.attempt().sequence().get() == next)
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn commit_terminal_loss(
    entry: &mut GroupConsumerEntry,
    transition: ClassicGroupTransition,
) -> Result<ClassicHeartbeatSuccessor, ClassicHeartbeatInterpretationFailure> {
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::Revoke {
        assignment,
        classic_generation,
    }) = effects.next()
    else {
        return Err(ClassicHeartbeatInterpretationFailure::PostCore(
            ClassicGroupExecutionError::HeartbeatTerminal,
        ));
    };
    if effects.next().is_some() {
        return Err(ClassicHeartbeatInterpretationFailure::PostCore(
            ClassicGroupExecutionError::HeartbeatTerminal,
        ));
    }
    match commit_revoke(entry, assignment, classic_generation) {
        Ok(()) => Ok(ClassicHeartbeatSuccessor::Dormant),
        Err(failure) => Err(ClassicHeartbeatInterpretationFailure::Revoke(failure)),
    }
}
