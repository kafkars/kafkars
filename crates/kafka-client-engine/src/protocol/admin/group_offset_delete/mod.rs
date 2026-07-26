//! Generated API-key 47 adaptation for one caller-ordered offset-deletion batch.

mod correlation;
mod model;
mod request;
mod response;
mod retention;

pub(crate) use model::{
    OffsetDeletePartitionRef, OffsetDeletePartitionResult, OffsetDeleteTargetRef,
    ValidatedOffsetDeleteResponse,
};
pub(crate) use request::{GroupOffsetDeleteRequestFailure, group_offset_delete_request};
pub(crate) use response::{
    GroupOffsetDeleteProtocolFailure, validate_group_offset_delete_response,
};

#[cfg(test)]
mod correlation_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
