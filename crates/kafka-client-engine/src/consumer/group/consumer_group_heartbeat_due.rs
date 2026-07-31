//! One broker-paced KIP-848 cadence observation per membership turn.

use kafka_client_core::Moment;

use crate::clock::MonotonicClock;

use super::{
    consumer_group_execution::ConsumerGroupExecutionError, registry::GroupConsumerRegistry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupHeartbeatDueTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn prepare_one_consumer_group_heartbeat(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
    ) -> Result<ConsumerGroupHeartbeatDueTurn, ConsumerGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && entry.fault.is_none()
                && entry.consumer.as_ref().is_some_and(|execution| {
                    execution
                        .machine()
                        .schedule()
                        .is_some_and(|schedule| schedule.deadline().is_elapsed_at(now))
                })
        }) else {
            return Ok(ConsumerGroupHeartbeatDueTurn::Idle);
        };
        let progressed = self.entries[index]
            .consumer
            .as_mut()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
            .prepare_due_heartbeat(now, clock)?;
        Ok(if progressed {
            ConsumerGroupHeartbeatDueTurn::Progress
        } else {
            ConsumerGroupHeartbeatDueTurn::Idle
        })
    }
}
