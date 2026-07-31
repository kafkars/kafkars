//! Tracked exact-broker execution of one final Fetch-session epoch.

use kafka_driver::{BrokerId, CompletionError, RoutedCall};
use kafka_wire::FetchResponse;

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchRequestSettings, FetchSessionRequest, fetch_session_close_request},
};

use super::submission::FetchSubmitError;

/// One accepted final-epoch request whose completion remains driver-owned.
#[must_use = "an accepted Fetch-session close must be polled or recovered"]
pub(crate) struct BrokerFetchCloseCall {
    call: RoutedCall<FetchResponse>,
}

impl BrokerFetchCloseCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: BrokerId,
        settings: FetchRequestSettings,
        session: FetchSessionRequest,
        deadline: OperationDeadline,
    ) -> Result<Self, BrokerFetchCloseSubmitError> {
        let request = fetch_session_close_request(settings, session)
            .ok_or(BrokerFetchCloseSubmitError::InvalidSession)?
            .map_err(|_error| BrokerFetchCloseSubmitError::Request)?;
        let call = driver
            .submit_tracked_broker_fetch(broker_id, request, deadline.transport())
            .map_err(BrokerFetchCloseSubmitError::Driver)?;
        Ok(Self { call })
    }

    /// Completes best-effort cleanup on any broker or transport terminal.
    pub(crate) fn poll(&mut self) -> Result<bool, CompletionError> {
        let Some(outcome) = self.call.try_result() else {
            return Ok(false);
        };
        let outcome = outcome?;
        let (result, _selected_version, route_token) = outcome.into_parts();
        drop((result, route_token));
        Ok(true)
    }
}

#[derive(Debug)]
pub(crate) enum BrokerFetchCloseSubmitError {
    InvalidSession,
    Request,
    Driver(FetchSubmitError),
}

impl BrokerFetchCloseSubmitError {
    pub(crate) const fn is_backpressured(&self) -> bool {
        matches!(
            self,
            Self::Driver(FetchSubmitError::Driver(kafka_driver::SubmitError::Full))
        )
    }
}
