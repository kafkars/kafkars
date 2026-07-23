//! Public producer records, options, admission handles, and ownership-aware results.

mod capture;
mod error;
mod handle;
mod prepare;
mod record;
mod result;
mod send;
mod send_error;

pub use super::pending::{ProducerSendFailure, ProducerSendFailureKind};
pub use capture::{
    ProducerSendCapture, ProducerSendCaptureError, ProducerSendCaptureErrorKind,
    ProducerSendOptions,
};
pub use error::{ProducerTrySendError, ProducerTrySendErrorKind};
pub use handle::ProducerHandle;
pub use record::{ProducerHeader as PublicProducerHeader, ProducerRecord as PublicProducerRecord};
pub use result::{ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerTrySendAccepted};
pub use send::ProducerSend;
pub use send_error::{
    ProducerSendError, ProducerSendResult, ProducerSendStartFailure, ProducerSendStartFailureKind,
};

#[cfg(test)]
mod capture_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod handle_send_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod prepare_test;
#[cfg(test)]
mod send_error_test;
#[cfg(test)]
mod send_test;
