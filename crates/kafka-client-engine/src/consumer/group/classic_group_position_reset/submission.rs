//! Original-deadline submission of one sequential reset lookup.

use kafka_client_core::{GroupPositionResetInput, Moment};

use super::{
    super::{
        classic_group_position::{
            ClassicGroupPositionExecution, ClassicGroupPositionExecutionError,
            ClassicGroupPositionExecutionState,
        },
        registry_entry::GroupConsumerEntry,
    },
    state::{ClassicGroupPositionResetDriverOwned, ClassicGroupPositionResetPrepared},
    transition::install_reset_transition,
};
use crate::{
    driver::{ClassicGroupPositionResetCall, DriverOwner},
    protocol::consumer::{ListOffsetsIsolation, list_offsets_request, remaining_timeout_ms},
};

pub(super) fn submit_reset(
    entry: &mut GroupConsumerEntry,
    driver: &DriverOwner,
    now: Moment,
) -> Result<(), ClassicGroupPositionExecutionError> {
    let state = entry
        .position
        .replace(ClassicGroupPositionExecutionState::Dormant);
    let ClassicGroupPositionExecutionState::ResetPrepared(mut prepared) = state else {
        entry.position.set(state);
        return Err(ClassicGroupPositionExecutionError::ResetNotPrepared);
    };
    if prepared.operation_deadline.core().is_elapsed_at(now) {
        let transition = match prepared
            .reset
            .apply(GroupPositionResetInput::DeadlineElapsed {
                fence: prepared.reset.fence(),
                now,
            }) {
            Ok(transition) => transition,
            Err(error) => {
                let kind = error.kind();
                entry
                    .position
                    .set(ClassicGroupPositionExecutionState::ResetPrepared(prepared));
                return Err(ClassicGroupPositionExecutionError::ResetCore(kind));
            }
        };
        return install_reset_transition(
            &mut entry.position,
            prepared.bootstrap,
            prepared.reset,
            prepared.operation_deadline,
            now,
            transition,
        );
    }
    let topic = match entry.catalog.copy_topic_name(prepared.partition.topic_id()) {
        Ok(topic) => topic,
        Err(error) => {
            settle_local_rejection(&mut entry.position, prepared, now)?;
            return Err(ClassicGroupPositionExecutionError::ResetCatalog(error));
        }
    };
    let isolation = match entry.read_isolation {
        kafka_client_core::ReadIsolation::ReadUncommitted => ListOffsetsIsolation::ReadUncommitted,
        kafka_client_core::ReadIsolation::ReadCommitted => ListOffsetsIsolation::ReadCommitted,
    };
    let Ok(call) = submit_reset_call(driver, &prepared, &topic, isolation, now) else {
        return settle_local_rejection(&mut entry.position, prepared, now);
    };
    let transition = match prepared
        .reset
        .apply(GroupPositionResetInput::DriverAccepted {
            fence: prepared.reset.fence(),
            partition: prepared.partition,
        }) {
        Ok(transition) => transition,
        Err(error) => {
            let kind = error.kind();
            install_driver_owned(&mut entry.position, prepared, topic, isolation, call);
            return Err(ClassicGroupPositionExecutionError::ResetCore(kind));
        }
    };
    let unexpected_effect = transition.into_effect().is_some();
    install_driver_owned(&mut entry.position, prepared, topic, isolation, call);
    if unexpected_effect {
        Err(ClassicGroupPositionExecutionError::ResetEffect)
    } else {
        Ok(())
    }
}

fn install_driver_owned(
    execution: &mut ClassicGroupPositionExecution,
    prepared: ClassicGroupPositionResetPrepared,
    topic: String,
    isolation: ListOffsetsIsolation,
    call: ClassicGroupPositionResetCall,
) {
    execution.set(ClassicGroupPositionExecutionState::ResetDriverOwned(
        ClassicGroupPositionResetDriverOwned {
            bootstrap: prepared.bootstrap,
            reset: prepared.reset,
            operation_deadline: prepared.operation_deadline,
            partition: prepared.partition,
            topic,
            isolation,
            call,
        },
    ));
}

fn submit_reset_call(
    driver: &DriverOwner,
    prepared: &ClassicGroupPositionResetPrepared,
    topic: &str,
    isolation: ListOffsetsIsolation,
    now: Moment,
) -> Result<ClassicGroupPositionResetCall, ()> {
    let timeout_ms = remaining_timeout_ms(now, prepared.operation_deadline.core()).map_err(drop)?;
    let request = list_offsets_request(
        topic,
        prepared.partition.partition(),
        prepared.position,
        isolation,
        timeout_ms,
    )
    .map_err(drop)?;
    let partition = i32::try_from(prepared.partition.partition().get()).map_err(drop)?;
    ClassicGroupPositionResetCall::submit(
        driver,
        topic,
        partition,
        request,
        prepared.operation_deadline.transport(),
    )
    .map_err(drop)
}

pub(super) fn settle_local_rejection(
    execution: &mut ClassicGroupPositionExecution,
    mut prepared: ClassicGroupPositionResetPrepared,
    now: Moment,
) -> Result<(), ClassicGroupPositionExecutionError> {
    let transition = match prepared
        .reset
        .apply(GroupPositionResetInput::DriverRejected {
            fence: prepared.reset.fence(),
            partition: prepared.partition,
            now,
        }) {
        Ok(transition) => transition,
        Err(error) => {
            let kind = error.kind();
            execution.set(ClassicGroupPositionExecutionState::ResetPrepared(prepared));
            return Err(ClassicGroupPositionExecutionError::ResetCore(kind));
        }
    };
    install_reset_transition(
        execution,
        prepared.bootstrap,
        prepared.reset,
        prepared.operation_deadline,
        now,
        transition,
    )
}
