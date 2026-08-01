//! Protocol normalization and two-phase forgotten-session settlement.

use crate::protocol::fetch::{ForgottenFetchOutcome, normalize_forgotten_fetch_outcome};

use super::{
    broker_maintenance_state::BrokerSessionMaintenance,
    broker_session::BrokerSessionPlan,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
};

impl DirectFetchExecutor {
    pub(super) fn settle_forgotten_terminal(
        &mut self,
        plan: BrokerSessionPlan,
        terminal: crate::driver::ForgottenFetchTerminal,
    ) -> Result<bool, FetchExecutionError> {
        let (request, _observed_at, selected_version, result, confirmation) = terminal.into_parts();
        let update = match (selected_version, result) {
            (Some(version), Ok(response)) => {
                match normalize_forgotten_fetch_outcome(request.session(), version, response) {
                    Ok(ForgottenFetchOutcome::Success { session, .. }) => Some(session),
                    Ok(ForgottenFetchOutcome::BrokerFailure(_)) | Err(_) => None,
                }
            }
            _ => None,
        };
        let settlement = match update {
            Some(update) => self
                .broker_sessions
                .as_mut()
                .unwrap_or_else(|| unreachable!("terminal requires session owner"))
                .complete(plan, update),
            None => self
                .broker_sessions
                .as_mut()
                .unwrap_or_else(|| unreachable!("terminal requires session owner"))
                .abort(plan, true),
        };
        if settlement.is_err() {
            self.broker_maintenance = Some(BrokerSessionMaintenance::ConfirmationFault {
                request,
                confirmation,
            });
            self.fault = Some(RetainedFetchFault::Registry);
            return Err(FetchExecutionError::BrokerSession);
        }
        confirmation.confirm();
        Ok(true)
    }

    pub(super) fn abort_maintenance_plan(
        &mut self,
        plan: BrokerSessionPlan,
        reset: bool,
    ) -> Result<(), FetchExecutionError> {
        self.broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("maintenance plan requires session owner"))
            .abort(plan, reset)
            .map_err(|_error| FetchExecutionError::BrokerSession)
    }
}
