//! Generated `LeaveGroup` adaptation for explicit static-member administration.

mod model;
mod request;
mod response;

pub(crate) use model::ValidatedRemoveConsumerGroupMembersResponse;
pub(crate) use request::{
    RemoveConsumerGroupMembersRequestFailure, remove_consumer_group_members_request,
    remove_consumer_group_members_request_charge,
};
pub(crate) use response::{
    RemoveConsumerGroupMembersProtocolFailure, validate_remove_consumer_group_members_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
