//! Flexible API-key 66 request construction and bounded response normalization.

mod materialize;
mod model;
mod request;
mod response;
mod retention;
mod validation;
mod version;

pub(crate) use model::{
    ListTransactionsRequestPlan, ListTransactionsResponseFacts, ListedTransaction,
};
#[cfg(test)]
pub(crate) use request::ListTransactionsRequestFailure;
pub(crate) use request::list_transactions_request;
pub(crate) use response::{ListTransactionsProtocolFailure, normalize_list_transactions_response};
#[cfg(test)]
pub(crate) use version::LIST_TRANSACTIONS_MAX_VERSION;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_failure_test;
#[cfg(test)]
mod response_success_test;
