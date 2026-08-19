//! Linear ownership of discovery and exact-broker group-listing calls.

mod admission;

use std::time::Instant;

use kafka_client_core::AdminGroupListingFilters;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{DescribeClusterResponse, ListGroupsResponse};

use crate::protocol::admin::{
    describe_cluster::describe_cluster_request, list_consumer_groups::list_consumer_groups_request,
};

use super::{
    super::DriverOwner,
    list_consumer_groups_terminal::{
        ListConsumerGroupsRawTerminal, retain_list_consumer_groups_broker_terminal,
        retain_list_consumer_groups_discovery_terminal,
    },
};

use admission::{
    ListConsumerGroupsBrokerAdmissionFailure, ListConsumerGroupsDiscoveryAdmissionFailure,
};

enum Inner {
    Discovery(Option<RoutedCall<DescribeClusterResponse>>),
    Broker {
        broker_id: i32,
        filters: Option<AdminGroupListingFilters>,
        retained_limit: usize,
        call: Option<RoutedCall<ListGroupsResponse>>,
    },
}

/// One accepted call retained beside its concrete operation owner.
#[must_use = "an accepted ListConsumerGroups call must be terminally settled"]
pub(crate) struct ListConsumerGroupsCall {
    inner: Inner,
    recovered: bool,
}

impl ListConsumerGroupsCall {
    pub(crate) fn submit_discovery(
        driver: &DriverOwner,
        deadline: Instant,
    ) -> Result<Self, ListConsumerGroupsDiscoveryAdmissionFailure> {
        let call = driver
            .submit_tracked_list_consumer_groups_discovery(
                describe_cluster_request(false, false),
                deadline,
            )
            .map_err(ListConsumerGroupsDiscoveryAdmissionFailure::new)?;
        Ok(Self {
            inner: Inner::Discovery(Some(call)),
            recovered: false,
        })
    }

    pub(crate) fn submit_broker(
        driver: &DriverOwner,
        broker_id: i32,
        filters: AdminGroupListingFilters,
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, ListConsumerGroupsBrokerAdmissionFailure> {
        let (request, minimum_version) =
            match list_consumer_groups_request(&filters, retained_limit) {
                Ok(request) => request,
                Err(source) => {
                    return Err(ListConsumerGroupsBrokerAdmissionFailure::request(
                        source,
                        broker_id,
                        filters,
                        retained_limit,
                    ));
                }
            };
        let call = match driver.submit_tracked_list_consumer_groups_broker(
            broker_id,
            request,
            minimum_version,
            deadline,
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(ListConsumerGroupsBrokerAdmissionFailure::driver(
                    source,
                    broker_id,
                    filters,
                    retained_limit,
                ));
            }
        };
        Ok(Self {
            inner: Inner::Broker {
                broker_id,
                filters: Some(filters),
                retained_limit,
                call: Some(call),
            },
            recovered: false,
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListConsumerGroupsRawTerminal, CompletionError>> {
        match &mut self.inner {
            Inner::Discovery(call) => {
                let result = call.as_mut()?.try_result()?;
                match result {
                    Ok(outcome) => {
                        drop(call.take());
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_consumer_groups_discovery_terminal(
                            selected_version,
                            result,
                            route_token,
                        )))
                    }
                    Err(source) => Some(Err(source)),
                }
            }
            Inner::Broker {
                broker_id,
                filters,
                retained_limit,
                call,
            } => {
                let result = call.as_mut()?.try_result()?;
                match result {
                    Ok(outcome) => {
                        let filters = filters.take()?;
                        drop(call.take());
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_consumer_groups_broker_terminal(
                            *broker_id,
                            filters,
                            *retained_limit,
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

    pub(crate) fn matches_discovery(&self) -> bool {
        matches!(self.inner, Inner::Discovery(_))
    }

    pub(crate) fn matches_broker(
        &self,
        expected_broker_id: i32,
        expected_filters: &AdminGroupListingFilters,
        expected_retained_limit: usize,
    ) -> bool {
        matches!(
            &self.inner,
            Inner::Broker {
                broker_id,
                filters: Some(filters),
                retained_limit,
                ..
            } if *broker_id == expected_broker_id
                && filters == expected_filters
                && *retained_limit == expected_retained_limit
        )
    }

    /// Converts unresolved ownership in place only after unique driver destruction.
    pub(crate) fn recover_after_driver_shutdown(&mut self) -> bool {
        if self.recovered {
            return true;
        }
        let retained = match &mut self.inner {
            Inner::Discovery(call) => call.take().is_some(),
            Inner::Broker { call, .. } => call.take().is_some(),
        };
        self.recovered = retained;
        retained
    }

    pub(crate) const fn is_recovered(&self) -> bool {
        self.recovered
    }

    /// Consumes exact recovered correlation after deterministic settlement.
    pub(crate) fn seal_recovered(self) {
        let Self { inner, recovered } = self;
        debug_assert!(recovered);
        match inner {
            Inner::Discovery(call) => drop(call),
            Inner::Broker { filters, call, .. } => {
                drop(filters);
                drop(call);
            }
        }
    }
}

#[cfg(test)]
mod call_test;
