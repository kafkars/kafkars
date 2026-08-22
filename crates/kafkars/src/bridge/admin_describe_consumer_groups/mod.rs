//! Declarative private bridge for consumer-group description.

mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeConsumerGroups;
pub(crate) use request::DescribeConsumerGroupsAdminRequest;

#[cfg(test)]
mod result_test;
