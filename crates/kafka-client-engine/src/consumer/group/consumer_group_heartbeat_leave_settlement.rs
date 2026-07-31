//! API 68 epoch-minus-one terminal validation and public close completion.

use kafka_client_core::ConsumerGroupHeartbeatFailure;

use crate::driver::{
    ConsumerGroupHeartbeatCompletionError, ConsumerGroupHeartbeatDriverFailureKind,
    ConsumerGroupHeartbeatResolution,
};

use super::{
    consumer_group_close::{complete_consumer_group_leave, fail_consumer_group_leave},
    consumer_group_execution::ConsumerGroupExecutionError,
    consumer_group_heartbeat_failure::{
        broker_close_terminal, completion_close_terminal, completion_failure,
        driver_close_terminal, driver_failure,
    },
    consumer_group_heartbeat_settlement::ConsumerGroupHeartbeatSettlementTurn,
    registry_entry::GroupConsumerEntry,
};

pub(super) fn settle_leave_completion_error(
    entry: &mut GroupConsumerEntry,
    error: ConsumerGroupHeartbeatCompletionError,
) -> Result<(), ConsumerGroupExecutionError> {
    fail_consumer_group_leave(
        entry,
        completion_failure(error),
        completion_close_terminal(error),
    )
}

pub(super) fn settle_leave_resolution(
    entry: &mut GroupConsumerEntry,
    resolution: ConsumerGroupHeartbeatResolution,
) -> Result<ConsumerGroupHeartbeatSettlementTurn, ConsumerGroupExecutionError> {
    match resolution {
        ConsumerGroupHeartbeatResolution::Succeeded(success) => {
            settle_leave_success(entry, success)?;
        }
        ConsumerGroupHeartbeatResolution::BrokerRejected { error_code, .. } => {
            fail_consumer_group_leave(
                entry,
                ConsumerGroupHeartbeatFailure::Broker(error_code),
                broker_close_terminal(error_code),
            )?;
        }
        ConsumerGroupHeartbeatResolution::Failed(failure) => {
            fail_consumer_group_leave(
                entry,
                driver_failure(failure),
                driver_close_terminal(failure),
            )?;
        }
    }
    Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
}

fn settle_leave_success(
    entry: &mut GroupConsumerEntry,
    success: crate::protocol::consumer::ConsumerGroupHeartbeatSuccess,
) -> Result<(), ConsumerGroupExecutionError> {
    let (_throttle, member, member_epoch, _interval, assignment) = success.into_parts();
    let member_matches = member.as_ref().is_none_or(|member| {
        entry
            .catalog
            .current_member()
            .is_some_and(|current| current.as_ref() == member.as_ref())
    });
    if member_matches && member_epoch == -1 && assignment.is_none() {
        return complete_consumer_group_leave(entry);
    }
    fail_consumer_group_leave(
        entry,
        ConsumerGroupHeartbeatFailure::InvalidResponse,
        driver_close_terminal(ConsumerGroupHeartbeatDriverFailureKind::InvalidResponse),
    )
}
