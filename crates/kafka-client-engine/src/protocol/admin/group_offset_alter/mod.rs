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
pub(crate) use response::GroupOffsetAlterProtocolFailure;

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
