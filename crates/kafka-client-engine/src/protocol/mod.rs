//! Wire-owned materialization behind engine-owned semantic inputs.

pub(crate) mod admin;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "direct-consumer host integration follows the protocol slices"
    )
)]
pub(crate) mod consumer;
mod error;
pub(crate) mod init_producer_id;
#[cfg(test)]
mod init_producer_id_test;
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
