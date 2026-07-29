//! Generated API-key 61 adaptation for one Admin `DescribeProducers` target.

mod correlation;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::{
    NormalizedDescribeProducerBrokerError, NormalizedDescribeProducerResult,
    NormalizedDescribeProducersResponse, NormalizedProducerState,
};
pub(crate) use request::describe_producers_request;
pub(crate) use response::{
    DescribeProducersProtocolFailure, normalize_describe_producers_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
