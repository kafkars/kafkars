//! Deterministic linear share-acknowledgement planning and range normalization.

mod error;
mod model;
mod normalize;
mod operation;

pub use error::{ShareAcknowledgementBuildError, ShareAcknowledgementBuildErrorKind};
pub use model::{
    ShareAcknowledgeType, ShareAcknowledgement, ShareAcknowledgementBatch, ShareDisposition,
    ShareRecordDecision,
};
pub use operation::{
    ShareAcknowledgementAdmission, ShareAcknowledgementApplyError,
    ShareAcknowledgementApplyErrorKind, ShareAcknowledgementFailureSettlement,
};

#[cfg(test)]
mod normalize_test;
