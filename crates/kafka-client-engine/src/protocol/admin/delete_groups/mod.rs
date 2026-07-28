//! Generated API-key 42 adaptation for one coordinator-routed consumer group.

mod model;
mod request;
mod response;

pub(crate) use model::NormalizedDeleteConsumerGroupsResponse;
pub(crate) use request::{
    delete_consumer_groups_request, delete_consumer_groups_request_peak_charge,
};
pub(crate) use response::{
    DeleteConsumerGroupsResponseFailure, normalize_delete_consumer_groups_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
