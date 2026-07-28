//! Declarative runtime-neutral receive boundary for hosted classic-group delivery.

mod completion;
mod error;
#[cfg(test)]
mod error_test;
mod operation;
mod signal;
mod signal_notification;
mod ticket;

pub(crate) use completion::{
    GroupConsumerRecvNotificationResources, GroupConsumerRecvNotifier, GroupConsumerRecvPublisher,
};
pub use error::{GroupConsumerRecvError, GroupConsumerRecvErrorKind};
pub use operation::GroupConsumerRecv;
pub(crate) use signal::{
    GroupConsumerRecvRegistration, GroupConsumerRecvSignal, GroupConsumerRecvSignalError,
    GroupConsumerRecvWait,
};
pub(crate) use ticket::GroupConsumerRecvTicket;
