//! Linear ownership of discovery and exact-broker Admin `ListTransactions` calls.

mod correlation;

use std::time::Instant;

use kafka_client_core::AdminListTransactionsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{DescribeClusterResponse, ListTransactionsRequest, ListTransactionsResponse};

use crate::protocol::admin::describe_cluster::describe_cluster_request;

use super::{
    super::DriverOwner,
    list_transactions_submission::ListTransactionsSubmitError,
    list_transactions_terminal::{
        ListTransactionsRawTerminal, retain_list_transactions_broker_terminal,
        retain_list_transactions_discovery_terminal,
    },
};

pub(super) use correlation::ListTransactionsCorrelation;

enum Inner {
    Discovery(Option<RoutedCall<DescribeClusterResponse>>),
    Broker(Option<RoutedCall<ListTransactionsResponse>>),
    Recovered,
}

/// One accepted call retained beside its concrete operation owner.
#[must_use = "an accepted ListTransactions call must be terminally settled"]
pub(crate) struct ListTransactionsCall {
    inner: Inner,
    correlation: Option<ListTransactionsCorrelation>,
}

impl ListTransactionsCall {
    pub(crate) fn submit_discovery(
        driver: &DriverOwner,
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, ListTransactionsCallAdmissionFailure> {
        let correlation = ListTransactionsCorrelation::discovery(retained_limit);
        let call = match driver.submit_tracked_list_transactions_discovery(
            describe_cluster_request(false, false),
            deadline,
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(ListTransactionsCallAdmissionFailure::new(
                    source,
                    correlation,
                ));
            }
        };
        Ok(Self {
            inner: Inner::Discovery(Some(call)),
            correlation: Some(correlation),
        })
    }

    pub(crate) fn submit_broker(
        driver: &DriverOwner,
        broker_id: i32,
        plan: AdminListTransactionsPlan,
        retained_limit: usize,
        request: ListTransactionsRequest,
        minimum_version: i16,
        deadline: Instant,
    ) -> Result<Self, ListTransactionsCallAdmissionFailure> {
        let correlation = ListTransactionsCorrelation::broker(broker_id, plan, retained_limit);
        let call = match driver.submit_tracked_list_transactions_broker(
            broker_id,
            request,
            minimum_version,
            deadline,
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(ListTransactionsCallAdmissionFailure::new(
                    source,
                    correlation,
                ));
            }
        };
        Ok(Self {
            inner: Inner::Broker(Some(call)),
            correlation: Some(correlation),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListTransactionsRawTerminal, CompletionError>> {
        match &mut self.inner {
            Inner::Discovery(call) => {
                let result = call.as_mut()?.try_result()?;
                match result {
                    Ok(outcome) => {
                        let correlation = self.correlation.take()?;
                        drop(call.take());
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_transactions_discovery_terminal(
                            selected_version,
                            result,
                            route_token,
                            correlation,
                        )))
                    }
                    Err(source) => Some(Err(source)),
                }
            }
            Inner::Broker(call) => {
                let result = call.as_mut()?.try_result()?;
                match result {
                    Ok(outcome) => {
                        let broker_id = self.correlation.as_ref()?.broker_id()?;
                        let correlation = self.correlation.take()?;
                        drop(call.take());
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_transactions_broker_terminal(
                            broker_id,
                            selected_version,
                            result,
                            route_token,
                            correlation,
                        )))
                    }
                    Err(source) => Some(Err(source)),
                }
            }
            Inner::Recovered => None,
        }
    }

    pub(crate) fn matches_discovery(&self, retained_limit: usize) -> bool {
        self.correlation
            .as_ref()
            .is_some_and(|correlation| correlation.matches_discovery(retained_limit))
    }

    pub(crate) fn matches_broker(
        &self,
        broker_id: i32,
        plan: &AdminListTransactionsPlan,
        retained_limit: usize,
    ) -> bool {
        self.correlation
            .as_ref()
            .is_some_and(|correlation| correlation.matches_broker(broker_id, plan, retained_limit))
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<Self> {
        let Self { inner, correlation } = self;
        let retained = match inner {
            Inner::Discovery(call) => call.is_some(),
            Inner::Broker(call) => call.is_some(),
            Inner::Recovered => false,
        };
        retained
            .then_some(correlation)
            .flatten()
            .map(|correlation| Self {
                inner: Inner::Recovered,
                correlation: Some(correlation),
            })
    }

    pub(crate) fn seal_recovered(self) {
        let Self { inner, correlation } = self;
        debug_assert!(matches!(inner, Inner::Recovered));
        drop(correlation);
    }
}

/// Definitely-unsent route, version-floor, or bounded-driver rejection.
#[must_use = "a rejected ListTransactions call must become operation input"]
pub(crate) struct ListTransactionsCallAdmissionFailure {
    source: ListTransactionsSubmitError,
    correlation: ListTransactionsCorrelation,
}

impl ListTransactionsCallAdmissionFailure {
    const fn new(
        source: ListTransactionsSubmitError,
        correlation: ListTransactionsCorrelation,
    ) -> Self {
        Self {
            source,
            correlation,
        }
    }

    pub(crate) fn into_submission_evidence(
        self,
    ) -> (Option<(i32, AdminListTransactionsPlan)>, usize) {
        let Self {
            source,
            correlation,
        } = self;
        drop(source);
        correlation.into_submission_evidence()
    }
}
