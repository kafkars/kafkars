//! Terminal settlement of linear broker Fetch-session plans.

use super::{executor::DirectFetchExecutor, fault::FetchExecutionError};

impl DirectFetchExecutor {
    pub(super) fn complete_broker_session(
        &mut self,
        fence: kafka_client_core::FetchFence,
        update: crate::protocol::fetch::FetchSessionUpdate,
    ) -> Result<bool, FetchExecutionError> {
        let Some(index) = self
            .active_broker_sessions
            .iter()
            .position(|active| active.fences.contains(&fence))
        else {
            return Ok(false);
        };
        let active = &mut self.active_broker_sessions[index];
        active.fences.retain(|active| *active != fence);
        match (active.update, update) {
            (_, crate::protocol::fetch::FetchSessionUpdate::Reset) => active.reset = true,
            (None, update) => active.update = Some(update),
            (Some(previous), update) if previous == update => {}
            (Some(_), _) => return Err(FetchExecutionError::BrokerSession),
        }
        if active.fences.is_empty() {
            let active = self.active_broker_sessions.swap_remove(index);
            let sessions = self
                .broker_sessions
                .as_mut()
                .unwrap_or_else(|| unreachable!("active broker plan requires session owner"));
            if active.reset {
                sessions
                    .abort(active.plan, true)
                    .map_err(|_error| FetchExecutionError::BrokerSession)?;
            } else {
                let update = active
                    .update
                    .unwrap_or(crate::protocol::fetch::FetchSessionUpdate::Reset);
                sessions
                    .complete(active.plan, update)
                    .map_err(|_error| FetchExecutionError::BrokerSession)?;
            }
        }
        Ok(true)
    }

    pub(super) fn abort_stale_broker_session(
        &mut self,
        fence: kafka_client_core::FetchFence,
    ) -> Result<(), FetchExecutionError> {
        let Some(index) = self
            .active_broker_sessions
            .iter()
            .position(|active| active.fences.contains(&fence))
        else {
            return Ok(());
        };
        let active = &mut self.active_broker_sessions[index];
        active.fences.retain(|active| *active != fence);
        active.reset = true;
        if !active.fences.is_empty() {
            return Ok(());
        }
        let active = self.active_broker_sessions.swap_remove(index);
        self.broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("active broker plan requires session owner"))
            .abort(active.plan, true)
            .map_err(|_error| FetchExecutionError::BrokerSession)
    }
}
