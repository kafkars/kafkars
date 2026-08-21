//! Declarative private bridge for concrete consumer-group offset operations.

mod alter_operation;
mod alter_request;
mod alter_result;
mod list_groups_operation;
mod list_groups_result;
mod list_operation;
mod list_request;
mod list_result;

pub(crate) use alter_operation::AdminAlterConsumerGroupOffsets;
pub(crate) use alter_request::AlterConsumerGroupOffsetsAdminRequest;
pub(crate) use list_groups_operation::AdminListConsumerGroupsOffsets;
pub(crate) use list_operation::AdminListConsumerGroupOffsets;
pub(crate) use list_request::{
    ListConsumerGroupOffsetsAdminRequest, ListConsumerGroupsOffsetsAdminRequest,
};

#[cfg(test)]
mod alter_operation_test;
#[cfg(test)]
mod alter_request_test;
#[cfg(test)]
mod alter_result_test;
#[cfg(test)]
mod list_operation_test;
#[cfg(test)]
mod list_request_test;
#[cfg(test)]
mod list_result_test;
