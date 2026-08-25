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
    classic_group_heartbeat_rejection::{install_heartbeat_effects, install_heartbeat_rejection},
    classic_group_rejection_fault::ClassicRejectionPostCore,
    classic_group_rejection_install::exact_broker_error,
    registry_entry::GroupConsumerEntry,
};

#[allow(
    clippy::large_enum_variant,
    reason = "failure variants retain exact post-core recovery ownership without hidden allocation"
)]
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
    coordinator_route_evidence: bool,
) -> Result<ClassicHeartbeatSuccessor, ClassicHeartbeatInterpretationFailure> {
    let key = terminal.key();
    if coordinator_route_evidence && terminal.coordinator_path_lost() {
        let transition = entry
            .classic
            .apply(ClassicGroupInput::HeartbeatCoordinatorLost {
                attempt: key.attempt(),
                now,
            })
            .map_err(|error| {
                ClassicHeartbeatInterpretationFailure::Restorable(ClassicGroupExecutionError::Core(
                    error.kind(),
                ))
            })?;
        install_heartbeat_rejection(entry, transition, now)
            .map_err(ClassicHeartbeatInterpretationFailure::PostCoreRejection)?;
        return Ok(ClassicHeartbeatSuccessor::Dormant);
    }
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
        return interpret_terminal_transition(entry, key.attempt(), transition, now);
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
            install_heartbeat_rejection(entry, transition, now)
                .map_err(ClassicHeartbeatInterpretationFailure::PostCoreRejection)?;
            return Ok(ClassicHeartbeatSuccessor::Dormant);
        }
        None => entry
            .classic
            .apply(ClassicGroupInput::HeartbeatFailed {
                attempt: key.attempt(),
                now,
            })
            .map_err(|error| {
                ClassicHeartbeatInterpretationFailure::Restorable(ClassicGroupExecutionError::Core(
                    error.kind(),
                ))
            })?,
    };
    interpret_terminal_transition(entry, key.attempt(), transition, now)
}

#[expect(
    clippy::result_large_err,
    reason = "the error retains exact post-core effects without allocating or erasing recovery state"
)]
fn interpret_terminal_transition(
    entry: &mut GroupConsumerEntry,
    attempt: kafka_client_core::ClassicHeartbeatAttempt,
    transition: ClassicGroupTransition,
    now: Moment,
) -> Result<ClassicHeartbeatSuccessor, ClassicHeartbeatInterpretationFailure> {
    let mut effects = transition.into_effects().take(2);
    let effects = [effects.next(), effects.next()];
    match effects {
        [Some(ClassicGroupEffect::ArmHeartbeat { schedule }), None] => {
            if successor_matches(attempt, schedule) {
                Ok(ClassicHeartbeatSuccessor::Waiting(schedule))
            } else {
                Err(ClassicHeartbeatInterpretationFailure::PostCore(
                    ClassicGroupExecutionError::HeartbeatTerminal,
                ))
            }
        }
        [
            Some(ClassicGroupEffect::Revoke {
                assignment,
                classic_generation,
            }),
            None,
        ] => match commit_revoke(entry, assignment, classic_generation) {
            Ok(()) => Ok(ClassicHeartbeatSuccessor::Dormant),
            Err(failure) => Err(ClassicHeartbeatInterpretationFailure::Revoke(failure)),
        },
        effects => {
            install_heartbeat_effects(entry, effects, now)
                .map_err(ClassicHeartbeatInterpretationFailure::PostCoreRejection)?;
            Ok(ClassicHeartbeatSuccessor::Dormant)
        }
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
