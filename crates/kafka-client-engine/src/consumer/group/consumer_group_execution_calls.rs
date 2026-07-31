//! Linear call-slot mutation for one KIP-848 execution owner.

use crate::driver::ConsumerGroupHeartbeatCall;

use super::{
    consumer_group_execution::{ConsumerGroupExecution, ConsumerGroupExecutionError},
    consumer_group_topic_identity_call::ConsumerGroupTopicIdentityCall,
};

impl ConsumerGroupExecution {
    pub(super) const fn topic_identity_call(&self) -> Option<&ConsumerGroupTopicIdentityCall> {
        self.topic_identity_call.as_ref()
    }

    pub(super) fn install_topic_identity_call(
        &mut self,
        call: ConsumerGroupTopicIdentityCall,
    ) -> Result<(), ConsumerGroupExecutionError> {
        if self.topic_identity_call.is_some() {
            return Err(ConsumerGroupExecutionError::TopicIdentityCallOccupied);
        }
        self.topic_identity_call = Some(call);
        Ok(())
    }

    pub(super) fn take_topic_identity_call(
        &mut self,
    ) -> Result<ConsumerGroupTopicIdentityCall, ConsumerGroupExecutionError> {
        self.topic_identity_call
            .take()
            .ok_or(ConsumerGroupExecutionError::TopicIdentityCallMissing)
    }

    pub(super) fn restore_topic_identity_call(
        &mut self,
        call: ConsumerGroupTopicIdentityCall,
    ) -> Result<(), ConsumerGroupExecutionError> {
        self.install_topic_identity_call(call)
    }

    pub(super) const fn heartbeat_call(&self) -> Option<&ConsumerGroupHeartbeatCall> {
        self.heartbeat_call.as_ref()
    }

    pub(super) fn install_heartbeat_call(
        &mut self,
        call: ConsumerGroupHeartbeatCall,
    ) -> Result<(), ConsumerGroupExecutionError> {
        if self.heartbeat_call.is_some() {
            return Err(ConsumerGroupExecutionError::HeartbeatCallOccupied);
        }
        self.heartbeat_call = Some(call);
        Ok(())
    }

    pub(super) fn take_heartbeat_call(
        &mut self,
    ) -> Result<ConsumerGroupHeartbeatCall, ConsumerGroupExecutionError> {
        self.heartbeat_call
            .take()
            .ok_or(ConsumerGroupExecutionError::HeartbeatCallMissing)
    }

    pub(super) fn restore_heartbeat_call(
        &mut self,
        call: ConsumerGroupHeartbeatCall,
    ) -> Result<(), ConsumerGroupExecutionError> {
        self.install_heartbeat_call(call)
    }

    pub(super) fn discard_calls_after_driver_shutdown(&mut self) {
        if let Some(call) = self.topic_identity_call.take() {
            call.discard_after_driver_shutdown();
        }
        if let Some(call) = self.heartbeat_call.take() {
            call.discard_after_driver_shutdown();
        }
    }
}
