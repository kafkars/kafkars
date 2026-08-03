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
        let active = &self.active_broker_sessions[index];
        let (next_update, next_reset) = match (active.update, update) {
            (_, crate::protocol::fetch::FetchSessionUpdate::Reset) => (active.update, true),
            (None, update) => (Some(update), active.reset),
            (Some(previous), update) if previous == update => (active.update, active.reset),
            (Some(_), _) => return Err(FetchExecutionError::BrokerSession),
        };
        if active.fences.len() == 1 {
            let sessions = self
                .broker_sessions
                .as_ref()
                .unwrap_or_else(|| unreachable!("active broker plan requires session owner"));
            let validation = if next_reset {
                sessions.validate_abort(&active.plan)
            } else {
                sessions.validate_complete(&active.plan)
            };
            validation.map_err(|_error| FetchExecutionError::BrokerSession)?;
        }
        let active = &mut self.active_broker_sessions[index];
        active.fences.retain(|active| *active != fence);
        active.update = next_update;
        active.reset = next_reset;
        if active.fences.is_empty() {
            let active = self.active_broker_sessions.swap_remove(index);
            let sessions = self
                .broker_sessions
                .as_mut()
                .unwrap_or_else(|| unreachable!("active broker plan requires session owner"));
            if active.reset {
                sessions.abort(active.plan, true).unwrap_or_else(|_error| {
                    unreachable!("validated broker Fetch-session abort must commit")
                });
            } else {
                let update = active
                    .update
                    .unwrap_or(crate::protocol::fetch::FetchSessionUpdate::Reset);
                sessions
                    .complete(active.plan, update)
                    .unwrap_or_else(|_error| {
                        unreachable!("validated broker Fetch-session completion must commit")
                    });
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
        let active = &self.active_broker_sessions[index];
        if active.fences.len() == 1 {
            self.broker_sessions
                .as_ref()
                .unwrap_or_else(|| unreachable!("active broker plan requires session owner"))
                .validate_abort(&active.plan)
                .map_err(|_error| FetchExecutionError::BrokerSession)?;
        }
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
            .unwrap_or_else(|_error| {
                unreachable!("validated stale broker Fetch-session abort must commit")
            });
        Ok(())
    }
}
