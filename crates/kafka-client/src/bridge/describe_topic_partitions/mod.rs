//! Declarative private bridge for one-page Admin `DescribeTopicPartitions`.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeTopicPartitions;
pub(crate) use request::DescribeTopicPartitionsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
