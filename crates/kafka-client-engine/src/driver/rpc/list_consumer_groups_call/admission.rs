//! Definitely-unsent discovery and exact-broker admission evidence.

use kafka_client_core::AdminGroupListingFilters;

use crate::protocol::admin::list_consumer_groups::ListConsumerGroupsRequestFailure;

use super::super::list_consumer_groups_submission::ListConsumerGroupsSubmitError;

/// Definitely-unsent discovery admission rejection.
#[must_use = "a rejected discovery call must become operation input"]
pub(crate) struct ListConsumerGroupsDiscoveryAdmissionFailure {
    source: ListConsumerGroupsSubmitError,
}

impl ListConsumerGroupsDiscoveryAdmissionFailure {
    pub(super) const fn new(source: ListConsumerGroupsSubmitError) -> Self {
        Self { source }
    }

    pub(crate) fn discard_source(self) {
        drop(self.source);
    }
}

/// Definitely-unsent exact-broker request or driver rejection.
#[must_use = "a rejected broker call must return its exact correlation"]
pub(crate) struct ListConsumerGroupsBrokerAdmissionFailure {
    source: ListConsumerGroupsCallAdmissionSource,
    broker_id: i32,
    filters: AdminGroupListingFilters,
    retained_limit: usize,
}

impl ListConsumerGroupsBrokerAdmissionFailure {
    pub(super) const fn request(
        source: ListConsumerGroupsRequestFailure,
        broker_id: i32,
        filters: AdminGroupListingFilters,
        retained_limit: usize,
    ) -> Self {
        Self {
            source: ListConsumerGroupsCallAdmissionSource::Request(source),
            broker_id,
            filters,
            retained_limit,
        }
    }

    pub(super) const fn driver(
        source: ListConsumerGroupsSubmitError,
        broker_id: i32,
        filters: AdminGroupListingFilters,
        retained_limit: usize,
    ) -> Self {
        Self {
            source: ListConsumerGroupsCallAdmissionSource::Driver(source),
            broker_id,
            filters,
            retained_limit,
        }
    }

    pub(crate) fn into_correlation(self) -> (i32, AdminGroupListingFilters, usize) {
        let Self {
            source,
            broker_id,
            filters,
            retained_limit,
        } = self;
        match source {
            ListConsumerGroupsCallAdmissionSource::Request(source) => {
                let _ = source;
            }
            ListConsumerGroupsCallAdmissionSource::Driver(source) => drop(source),
        }
        (broker_id, filters, retained_limit)
    }
}

enum ListConsumerGroupsCallAdmissionSource {
    Request(ListConsumerGroupsRequestFailure),
    Driver(ListConsumerGroupsSubmitError),
}
