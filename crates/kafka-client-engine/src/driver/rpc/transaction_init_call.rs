//! Linear ownership of one accepted transaction-coordinator identity call.

use std::{error::Error, fmt, time::Instant};

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::InitProducerIdResponse;

use crate::protocol::transaction::transaction_init_request;

use super::{
    super::DriverOwner,
    transaction_init_submission::TransactionInitSubmitError,
    transaction_init_terminal::{TransactionInitTerminal, retain_transaction_init_terminal},
};

#[must_use = "an accepted transaction initialization call requires terminal settlement"]
pub(crate) struct TransactionInitCall {
    call: Option<RoutedCall<InitProducerIdResponse>>,
}

impl TransactionInitCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        transaction_timeout_ms: u32,
        deadline: Instant,
    ) -> Result<Self, TransactionInitCallAdmissionFailure> {
        let request = transaction_init_request(transactional_id, transaction_timeout_ms);
        let call = driver
            .submit_tracked_transaction_init(transactional_id, request, deadline)
            .map_err(TransactionInitCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<TransactionInitTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_transaction_init_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(error) => Some(Err(error)),
        }
    }

    pub(crate) fn discard_after_driver_shutdown(mut self) {
        drop(self.call.take());
    }
}

#[must_use = "a rejected call must become a definitely-unsent core fact"]
#[derive(Debug)]
pub(crate) enum TransactionInitCallAdmissionFailure {
    Driver(TransactionInitSubmitError),
}

impl fmt::Display for TransactionInitCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionInitCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}
