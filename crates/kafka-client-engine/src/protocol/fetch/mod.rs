//! Bounded Fetch-response normalization into engine-owned retained records.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "direct-consumer host interpretation follows this bounded protocol seam"
    )
)]

mod batch;
mod batch_identity;
mod batch_model;
mod control_record;
mod decode;
mod exports;
mod failure;
#[cfg(test)]
pub(crate) mod fixture;
mod isolation;
mod limits;
mod model;
mod outcome;
mod outcome_failure;
#[cfg(test)]
mod outcome_failure_test;
mod outcome_normalize;
mod outcome_retain;
mod read_committed;
mod record_failure;
#[cfg(test)]
mod record_failure_test;
mod request;
mod response;
mod retention;

pub(crate) use exports::*;
#[cfg(test)]
pub(crate) use fixture::{
    encoded_data_batch_for_test, encoded_delivery_batches_for_test,
    retained_broker_failure_for_test, retained_success_for_test,
};
#[cfg(test)]
mod batch_identity_test;
#[cfg(test)]
mod batch_model_test;
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod control_record_test;
#[cfg(test)]
mod decode_next_test;
#[cfg(test)]
mod decode_test;
#[cfg(test)]
mod facts_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod isolation_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_broker_test;
#[cfg(test)]
mod outcome_offset_test;
#[cfg(test)]
mod outcome_read_committed_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod read_committed_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
