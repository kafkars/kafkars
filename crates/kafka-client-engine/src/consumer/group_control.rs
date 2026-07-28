//! Declarative exact-input batch-control boundary for one group consumer.

mod accepted;
mod error;
mod operation;
mod partition;
mod resume_capture;
#[cfg(test)]
mod resume_capture_test;

pub use accepted::{GroupConsumerControlAccepted, GroupConsumerControlAcceptedFaultKind};
pub use error::{GroupConsumerControlError, GroupConsumerControlErrorKind};
pub use partition::{
    GroupConsumerPartition, GroupConsumerPartitionInputError, GroupConsumerPartitionInputErrorKind,
};
pub use resume_capture::{
    GroupConsumerResumeCapture, GroupConsumerResumeCaptureError,
    GroupConsumerResumeCaptureErrorKind,
};
