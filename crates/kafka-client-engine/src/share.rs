//! Curated low-level Rust API for hosted share-member ownership.

pub use crate::consumer::{
    ShareConsumerAssignmentPartition, ShareConsumerClose, ShareConsumerCloseAdmissionError,
    ShareConsumerCloseAdmissionErrorKind, ShareConsumerCloseError, ShareConsumerCloseErrorKind,
    ShareConsumerHandle, ShareConsumerRegistration, ShareConsumerRegistrationError,
    ShareConsumerRegistrationErrorKind, ShareConsumerStartCapture, ShareConsumerState,
    ShareConsumerStateError, ShareConsumerStateErrorKind,
};
