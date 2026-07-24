//! Internal transfer joining a catalog name with one exact Fetch lease.

use std::sync::Arc;

use crate::consumer::fetch_store::FetchDelivery;

/// Engine-owned application delivery before its public batch wrapper exists.
#[must_use = "an assigned delivery must enter a public batch or return to its owner"]
pub(crate) struct AssignedConsumerDelivery {
    topic: Arc<str>,
    partition: i32,
    lease: FetchDelivery,
}

impl AssignedConsumerDelivery {
    pub(crate) fn new(topic: Arc<str>, partition: i32, lease: FetchDelivery) -> Self {
        Self {
            topic,
            partition,
            lease,
        }
    }

    pub(super) fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(super) const fn lease(&self) -> &FetchDelivery {
        &self.lease
    }

    pub(crate) fn into_lease(self) -> FetchDelivery {
        self.lease
    }
}
