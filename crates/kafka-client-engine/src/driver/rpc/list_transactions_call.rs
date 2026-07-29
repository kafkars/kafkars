//! Linear ownership of discovery and exact-broker Admin `ListTransactions` calls.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{DescribeClusterResponse, ListTransactionsRequest, ListTransactionsResponse};

use crate::protocol::admin::describe_cluster::describe_cluster_request;

use super::{
    super::DriverOwner,
    list_transactions_submission::ListTransactionsSubmitError,
    list_transactions_terminal::{
        ListTransactionsRawTerminal, RecoveredListTransactionsCall,
        retain_list_transactions_broker_terminal, retain_list_transactions_discovery_terminal,
    },
};

enum Inner {
    Discovery(Option<RoutedCall<DescribeClusterResponse>>),
    Broker {
        broker_id: i32,
        call: Option<RoutedCall<ListTransactionsResponse>>,
    },
}

/// One accepted call retained beside its concrete operation owner.
#[must_use = "an accepted ListTransactions call must be terminally settled"]
pub(crate) struct ListTransactionsCall {
    inner: Inner,
}

impl ListTransactionsCall {
    pub(crate) fn submit_discovery(
        driver: &DriverOwner,
        deadline: Instant,
    ) -> Result<Self, ListTransactionsCallAdmissionFailure> {
        let call = driver
            .submit_tracked_list_transactions_discovery(
                describe_cluster_request(false, false),
                deadline,
            )
            .map_err(ListTransactionsCallAdmissionFailure::new)?;
        Ok(Self {
            inner: Inner::Discovery(Some(call)),
        })
    }

    pub(crate) fn submit_broker(
        driver: &DriverOwner,
        broker_id: i32,
        request: ListTransactionsRequest,
        minimum_version: i16,
        deadline: Instant,
    ) -> Result<Self, ListTransactionsCallAdmissionFailure> {
        let call = driver
            .submit_tracked_list_transactions_broker(broker_id, request, minimum_version, deadline)
            .map_err(ListTransactionsCallAdmissionFailure::new)?;
        Ok(Self {
            inner: Inner::Broker {
                broker_id,
                call: Some(call),
            },
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListTransactionsRawTerminal, CompletionError>> {
        match &mut self.inner {
            Inner::Discovery(call) => {
                let result = call.as_mut()?.try_result()?;
                drop(call.take());
                match result {
                    Ok(outcome) => {
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_transactions_discovery_terminal(
                            selected_version,
                            result,
                            route_token,
                        )))
                    }
                    Err(source) => Some(Err(source)),
                }
            }
            Inner::Broker { broker_id, call } => {
                let result = call.as_mut()?.try_result()?;
                drop(call.take());
                match result {
                    Ok(outcome) => {
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_transactions_broker_terminal(
                            *broker_id,
                            selected_version,
                            result,
                            route_token,
                        )))
                    }
                    Err(source) => Some(Err(source)),
                }
            }
        }
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredListTransactionsCall> {
        let retained = match self.inner {
            Inner::Discovery(call) => call.is_some(),
            Inner::Broker { call, .. } => call.is_some(),
        };
        retained.then(RecoveredListTransactionsCall::new)
    }
}

/// Definitely-unsent route, version-floor, or bounded-driver rejection.
#[must_use = "a rejected ListTransactions call must become operation input"]
pub(crate) struct ListTransactionsCallAdmissionFailure {
    source: ListTransactionsSubmitError,
}

impl ListTransactionsCallAdmissionFailure {
    const fn new(source: ListTransactionsSubmitError) -> Self {
        Self { source }
    }

    pub(crate) fn into_source(self) -> ListTransactionsSubmitError {
        self.source
    }
}
