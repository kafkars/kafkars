//! Declarative runtime-neutral receive boundary for hosted share delivery.

mod completion;
mod error;
mod operation;
mod signal;
mod signal_notification;
mod ticket;

#[cfg(test)]
mod error_test;
#[cfg(test)]
mod operation_test;

pub(crate) use completion::{
    ShareConsumerRecvNotificationResources, ShareConsumerRecvNotifier, ShareConsumerRecvPublisher,
};
pub use error::{ShareConsumerRecvError, ShareConsumerRecvErrorKind};
pub use operation::ShareConsumerRecv;
pub(crate) use signal::{
    ShareConsumerRecvRegistration, ShareConsumerRecvSignal, ShareConsumerRecvSignalError,
    ShareConsumerRecvWait,
};
pub(crate) use ticket::ShareConsumerRecvTicket;
