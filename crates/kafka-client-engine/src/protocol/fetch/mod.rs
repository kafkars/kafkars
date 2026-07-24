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
mod decode;
mod exports;
mod failure;
mod limits;
mod model;
mod outcome;
mod outcome_normalize;
mod outcome_retain;
mod request;
mod response;
mod retention;

pub(crate) use exports::*;

#[cfg(test)]
mod batch_identity_test;
#[cfg(test)]
mod batch_model_test;
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod decode_next_test;
#[cfg(test)]
mod decode_test;
#[cfg(test)]
mod facts_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod limits_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod outcome_broker_test;
#[cfg(test)]
mod outcome_offset_test;
#[cfg(test)]
mod outcome_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
