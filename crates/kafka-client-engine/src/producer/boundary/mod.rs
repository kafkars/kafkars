//! Public producer records, options, admission handles, and ownership-aware results.

mod capture;
mod error;
mod handle;
mod record;
mod result;

pub use capture::{
    ProducerSendCapture, ProducerSendCaptureError, ProducerSendCaptureErrorKind,
    ProducerSendOptions,
};
pub use error::{ProducerTrySendError, ProducerTrySendErrorKind};
pub use handle::ProducerHandle;
pub use record::{ProducerHeader as PublicProducerHeader, ProducerRecord as PublicProducerRecord};
pub use result::{
    ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerOperationId, ProducerTrySendAccepted,
};

#[cfg(test)]
mod capture_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod handle_test;
