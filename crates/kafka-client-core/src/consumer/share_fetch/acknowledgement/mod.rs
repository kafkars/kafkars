//! Deterministic linear share-acknowledgement planning and range normalization.

mod error;
mod model;
mod normalize;

pub use error::{ShareAcknowledgementBuildError, ShareAcknowledgementBuildErrorKind};
pub use model::{
    ShareAcknowledgeType, ShareAcknowledgement, ShareAcknowledgementBatch, ShareDisposition,
    ShareRecordDecision,
};

#[cfg(test)]
mod normalize_test;
