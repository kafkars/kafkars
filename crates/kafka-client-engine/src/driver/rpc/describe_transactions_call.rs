//! Linear ownership of one accepted coordinator-routed Admin `DescribeTransactions` call.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeTransactionsResponse;

use crate::protocol::admin::describe_transactions::describe_transactions_request;

use super::{
    super::DriverOwner,
    describe_transactions_terminal::{
        DescribeTransactionsRawTerminal, RecoveredDescribeTransactionsCall,
        retain_describe_transactions_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeTransactions call must be terminally settled"]
pub(crate) struct DescribeTransactionsCall {
    call: Option<RoutedCall<DescribeTransactionsResponse>>,
}

impl DescribeTransactionsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        transactional_id: &str,
        deadline: Instant,
    ) -> Result<Self, DescribeTransactionsCallAdmissionFailure> {
        let request = describe_transactions_request(transactional_id);
        let call = driver
            .submit_tracked_describe_transactions(transactional_id, request, deadline)
            .map_err(|_source| DescribeTransactionsCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing its coordinator evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeTransactionsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_transactions_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        mut self,
    ) -> Option<RecoveredDescribeTransactionsCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredDescribeTransactionsCall
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected DescribeTransactions call must become operation input"]
pub(crate) struct DescribeTransactionsCallAdmissionFailure;
