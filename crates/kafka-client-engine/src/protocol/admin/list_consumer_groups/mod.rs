//! Generated discovery and exactly filtered `ListGroups` request/response seam.

mod discovery;
mod request;
mod response;

pub(crate) use discovery::{
    NormalizedListConsumerGroupsDiscovery, normalize_list_consumer_groups_discovery,
};
pub(crate) use request::{ListConsumerGroupsRequestFailure, list_consumer_groups_request};
pub(crate) use response::{
    ListConsumerGroupsProtocolFailure, normalize_list_consumer_groups_response,
};

#[cfg(test)]
mod discovery_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
