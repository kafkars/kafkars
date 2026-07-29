//! Generated API-key 65 adaptation for one Admin `DescribeTransactions` ID.

mod correlation;
mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;

pub(crate) use model::{
    NormalizedDescribeTransactionBrokerError, NormalizedDescribeTransactionDescription,
    NormalizedDescribeTransactionResult, NormalizedDescribeTransactionTopic,
    NormalizedDescribeTransactionsResponse,
};
pub(crate) use request::describe_transactions_request;
pub(crate) use response::{
    DescribeTransactionsProtocolFailure, normalize_describe_transactions_response,
};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
