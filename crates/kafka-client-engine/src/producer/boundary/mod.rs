//! Public producer records, options, admission handles, and ownership-aware results.

mod cancellation;
mod capture;
mod close;
mod error;
mod flush_error;
mod flush_result;
mod handle;
mod prepare;
mod record;
mod result;

pub use cancellation::{
    ProducerCancelAccepted, ProducerCancelError, ProducerCancelErrorKind, ProducerCancelFault,
    ProducerCancelFaultKind, ProducerCancellationOutcome,
};
pub use capture::{
    ProducerSendCapture, ProducerSendCaptureError, ProducerSendCaptureErrorKind,
    ProducerSendOptions,
};
pub use close::{ProducerTryCloseAccepted, ProducerTryCloseError, ProducerTryCloseErrorKind};
pub use error::{ProducerTrySendError, ProducerTrySendErrorKind};
pub use flush_error::{ProducerTryFlushError, ProducerTryFlushErrorKind};
pub use flush_result::ProducerTryFlushAccepted;
pub use handle::ProducerHandle;
pub use record::{ProducerHeader as PublicProducerHeader, ProducerRecord as PublicProducerRecord};
pub use result::{ProducerAcceptedFault, ProducerAcceptedFaultKind, ProducerTrySendAccepted};

#[cfg(test)]
mod cancellation_test;
#[cfg(test)]
mod capture_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod flush_error_test;
#[cfg(test)]
mod flush_result_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod prepare_test;
