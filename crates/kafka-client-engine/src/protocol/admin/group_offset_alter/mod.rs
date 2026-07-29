//! Generated API-key 8 adaptation for one caller-ordered offset alteration.

mod correlation;
mod model;
mod request;
mod response;
mod retention;
mod shape;
mod version;

pub(crate) use model::{
    OffsetCommitPartitionRef, OffsetCommitPartitionResult, OffsetCommitTargetRef,
    ValidatedOffsetCommitResponse,
};
pub(crate) use request::{GroupOffsetAlterRequestFailure, group_offset_alter_request};
pub(crate) use response::{GroupOffsetAlterProtocolFailure, validate_group_offset_alter_response};
pub(crate) use retention::generated_request_peak_charge;
pub(crate) use version::{group_offset_alter_maximum_version, group_offset_alter_minimum_version};

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
#[cfg(test)]
mod shape_test;
#[cfg(test)]
mod version_test;
