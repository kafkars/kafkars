//! Wire-owned materialization behind engine-owned semantic inputs.

mod error;
pub(crate) mod produce;
#[cfg(test)]
mod produce_batch_test;
pub(crate) mod produce_failure;
#[cfg(test)]
mod produce_failure_test;
#[cfg(test)]
mod produce_test;
