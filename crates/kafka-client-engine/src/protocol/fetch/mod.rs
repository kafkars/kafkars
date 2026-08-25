//! Bounded Fetch-response normalization into engine-owned retained records.
#![allow(dead_code, reason = "private Fetch seam")]
mod batch;
mod batch_identity;
mod batch_model;
mod control_record;
mod decode;
mod evidence;
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
mod outcome_forgotten;
#[cfg(test)]
mod outcome_forgotten_test;
mod outcome_normalize;
mod outcome_retain;
mod read_committed;
mod record_failure;
#[cfg(test)]
mod record_failure_test;
mod request;
mod request_broker;
mod response;
mod retention;
mod session;
pub(crate) use exports::*;
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
mod evidence_test;
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
mod request_broker_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod session_test;
