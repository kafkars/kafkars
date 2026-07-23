//! Wire-owned materialization behind engine-owned semantic inputs.

pub(crate) mod admin;
mod error;
pub(crate) mod produce;
#[cfg(test)]
mod produce_batch_test;
pub(crate) mod produce_failure;
#[cfg(test)]
mod produce_failure_test;
pub(crate) mod produce_outcome;
#[cfg(test)]
mod produce_outcome_test;
pub(crate) mod produce_response;
#[cfg(test)]
mod produce_response_test;
#[cfg(test)]
mod produce_test;
