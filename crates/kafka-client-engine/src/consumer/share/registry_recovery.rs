//! Driver-shutdown release of every share call, member, and close observer.

use kafka_client_core::{ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase};

use crate::completion::CompletionRegistryError;

use super::{
    close_state::ShareConsumerCloseTerminal, registry::ShareConsumerRegistry,
    registry_membership::ShareMembershipHostError,
};

impl ShareConsumerRegistry {
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> Result<(), ShareMembershipHostError> {
        self.close_admission();
        self.invalidations.discard_share_after_driver_shutdown();
        while let Some(mut entry) = self.entries.pop() {
            drop(entry.topic_call.take());
            drop(entry.heartbeat_call.take());
            if let Some(routing) = entry.fetch_mut().routing_mut() {
                let _recovered = routing.recover_after_driver_shutdown();
            }
            drop(entry.fetch_mut().take_routing());
            drop(entry.fetch_mut().take_routed());
            if let Some(sessions) = entry.fetch_mut().take_sessions() {
                sessions
                    .recover_after_driver_shutdown()
                    .map_err(|_error| ShareMembershipHostError::EffectShape)?;
            }
            if let Some(membership) = &mut entry.membership
                && membership.machine().phase() != ShareGroupHeartbeatPhase::Closed
            {
                membership.close_locally()?;
            }
            if let Some(completion_id) = entry
                .close()
                .and_then(super::close_state::ShareConsumerCloseState::completion_id)
            {
                self.close_completions
                    .publish(
                        completion_id,
                        ShareConsumerCloseTerminal::Failed(ShareGroupHeartbeatFailure::Execution),
                    )
                    .map_err(|(error, _terminal)| completion_failure(error))?;
            }
            self.retained_name_bytes = self
                .retained_name_bytes
                .checked_sub(entry.retained_name_bytes())
                .ok_or(ShareMembershipHostError::EffectShape)?;
            drop(entry);
        }
        Ok(())
    }
}

const fn completion_failure(_error: CompletionRegistryError) -> ShareMembershipHostError {
    ShareMembershipHostError::EffectShape
}
