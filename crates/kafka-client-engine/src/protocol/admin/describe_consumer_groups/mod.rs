//! Generated classic API-key 15 and KIP-848 API-key 69 group-description adaptation.

mod modern_assignment;
mod modern_model;
mod modern_outcome;
mod modern_request;
mod modern_response;
mod modern_response_duplicates;
mod modern_response_validation;
mod modern_response_value;
mod request;
mod response;

pub(crate) use modern_assignment::{
    ConsumerGroupDescribeAssignment, ConsumerGroupDescribeTopicPartitions,
};
pub(crate) use modern_model::{ConsumerGroupDescribeDescription, ConsumerGroupDescribeMember};
#[cfg(test)]
pub(crate) use modern_outcome::ConsumerGroupDescribeFallback;
pub(crate) use modern_outcome::ConsumerGroupDescribeResult;
pub(crate) use modern_request::{
    ConsumerGroupDescribeRequestFailure, consumer_group_describe_request,
};
pub(crate) use modern_response::{
    ConsumerGroupDescribeResponseFailure, normalize_consumer_group_describe_response,
};
pub(crate) use request::describe_consumer_group_request;
pub(crate) use response::{
    DescribeConsumerGroupResponseFailure, normalize_describe_consumer_group_response,
};

#[cfg(test)]
mod modern_request_test;
#[cfg(test)]
mod modern_response_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
