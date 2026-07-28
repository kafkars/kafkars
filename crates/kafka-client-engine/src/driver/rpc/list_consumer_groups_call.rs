//! Linear ownership of discovery and exact-broker group-listing calls.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{DescribeClusterResponse, ListGroupsResponse};

use crate::protocol::admin::{
    describe_cluster::describe_cluster_request, list_consumer_groups::list_consumer_groups_request,
};

use super::{
    super::DriverOwner,
    list_consumer_groups_submission::ListConsumerGroupsSubmitError,
    list_consumer_groups_terminal::{
        ListConsumerGroupsRawTerminal, RecoveredListConsumerGroupsCall,
        retain_list_consumer_groups_broker_terminal,
        retain_list_consumer_groups_discovery_terminal,
    },
};

enum Inner {
    Discovery(Option<RoutedCall<DescribeClusterResponse>>),
    Broker {
        broker_id: i32,
        call: Option<RoutedCall<ListGroupsResponse>>,
    },
}

/// One accepted call retained beside its concrete operation owner.
#[must_use = "an accepted ListConsumerGroups call must be terminally settled"]
pub(crate) struct ListConsumerGroupsCall {
    inner: Inner,
}

impl ListConsumerGroupsCall {
    pub(crate) fn submit_discovery(
        driver: &DriverOwner,
        deadline: Instant,
    ) -> Result<Self, ListConsumerGroupsCallAdmissionFailure> {
        let call = driver
            .submit_tracked_list_consumer_groups_discovery(describe_cluster_request(), deadline)
            .map_err(ListConsumerGroupsCallAdmissionFailure::new)?;
        Ok(Self {
            inner: Inner::Discovery(Some(call)),
        })
    }

    pub(crate) fn submit_broker(
        driver: &DriverOwner,
        broker_id: i32,
        deadline: Instant,
    ) -> Result<Self, ListConsumerGroupsCallAdmissionFailure> {
        let call = driver
            .submit_tracked_list_consumer_groups_broker(
                broker_id,
                list_consumer_groups_request(),
                deadline,
            )
            .map_err(ListConsumerGroupsCallAdmissionFailure::new)?;
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
    ) -> Option<Result<ListConsumerGroupsRawTerminal, CompletionError>> {
        match &mut self.inner {
            Inner::Discovery(call) => {
                let result = call.as_mut()?.try_result()?;
                drop(call.take());
                match result {
                    Ok(outcome) => {
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
            Inner::Broker { broker_id, call } => {
                let result = call.as_mut()?.try_result()?;
                drop(call.take());
                match result {
                    Ok(outcome) => {
                        let (result, selected_version, route_token) = outcome.into_parts();
                        Some(Ok(retain_list_consumer_groups_broker_terminal(
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
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredListConsumerGroupsCall> {
        let retained = match self.inner {
            Inner::Discovery(call) => call.is_some(),
            Inner::Broker { call, .. } => call.is_some(),
        };
        retained.then(RecoveredListConsumerGroupsCall::new)
    }
}

/// Definitely-unsent route validation or bounded-driver rejection.
#[must_use = "a rejected ListConsumerGroups call must become operation input"]
pub(crate) struct ListConsumerGroupsCallAdmissionFailure {
    source: ListConsumerGroupsSubmitError,
}

impl ListConsumerGroupsCallAdmissionFailure {
    const fn new(source: ListConsumerGroupsSubmitError) -> Self {
        Self { source }
    }

    pub(crate) fn into_source(self) -> ListConsumerGroupsSubmitError {
        self.source
    }
}
