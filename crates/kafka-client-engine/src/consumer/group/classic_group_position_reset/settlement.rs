//! Exact route-retaining settlement of one sequential reset lookup.

use kafka_client_core::{GroupPositionResetInput, Moment};

use super::{
    super::{
        classic_group_position::{
            ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
        },
        registry_entry::GroupConsumerEntry,
    },
    state::{
        ClassicGroupPositionResetCompletionFault, ClassicGroupPositionResetDriverOwned,
        ClassicGroupPositionResetTerminalFault,
    },
    transition::install_reset_transition,
};
use crate::driver::{
    ClassicGroupPositionResetCompletionError, ClassicGroupPositionResetOutcome,
    ListOffsetsResolution,
};

pub(super) fn settle_reset(
    entry: &mut GroupConsumerEntry,
    now: Moment,
    result: Result<ClassicGroupPositionResetOutcome, ClassicGroupPositionResetCompletionError>,
) -> Result<(), ClassicGroupPositionExecutionError> {
    let state = entry
        .position
        .replace(ClassicGroupPositionExecutionState::Dormant);
    let ClassicGroupPositionExecutionState::ResetDriverOwned(owner) = state else {
        entry.position.set(state);
        return Err(ClassicGroupPositionExecutionError::ResetNotDriverOwned);
    };
    let ClassicGroupPositionResetDriverOwned {
        bootstrap,
        mut reset,
        operation_deadline,
        partition,
        topic,
        isolation,
        call,
    } = owner;
    drop(call);
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(source) => {
            entry
                .position
                .set(ClassicGroupPositionExecutionState::ResetCompletionFault(
                    ClassicGroupPositionResetCompletionFault {
                        _bootstrap: bootstrap,
                        _reset: reset,
                        _operation_deadline: operation_deadline,
                        _source: source,
                    },
                ));
            return Err(ClassicGroupPositionExecutionError::ResetCompletion(source));
        }
    };
    let (terminal, route) = outcome.into_resolution(&topic, partition.partition(), isolation);
    let input = match terminal {
        ListOffsetsResolution::Resolved {
            next_offset,
            throttle_time_ms,
        } => GroupPositionResetInput::OffsetResolved {
            fence: reset.fence(),
            partition,
            now,
            next_offset,
            throttle_time_ms,
        },
        ListOffsetsResolution::Failed(failure) => GroupPositionResetInput::ResolutionFailed {
            fence: reset.fence(),
            partition,
            now,
            failure,
        },
    };
    let transition = match reset.apply(input) {
        Ok(transition) => transition,
        Err(error) => {
            let kind = error.kind();
            entry
                .position
                .set(ClassicGroupPositionExecutionState::ResetTerminalFault(
                    ClassicGroupPositionResetTerminalFault {
                        _bootstrap: bootstrap,
                        _reset: reset,
                        _operation_deadline: operation_deadline,
                        _partition: partition,
                        _terminal: terminal,
                        _route: route,
                    },
                ));
            return Err(ClassicGroupPositionExecutionError::ResetCore(kind));
        }
    };
    drop(route);
    install_reset_transition(
        &mut entry.position,
        bootstrap,
        reset,
        operation_deadline,
        now,
        transition,
    )
}
