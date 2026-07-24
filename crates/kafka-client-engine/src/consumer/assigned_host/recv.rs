//! Runtime-neutral waiting for one already-authorized assigned-consumer batch.

mod error;
mod operation;
mod port;
mod signal;
mod ticket;

#[cfg(test)]
mod error_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod port_test;
#[cfg(test)]
mod signal_test;
#[cfg(test)]
mod ticket_test;

pub use error::{AssignedConsumerRecvError, AssignedConsumerRecvErrorKind};
pub use operation::AssignedConsumerRecv;
pub(crate) use signal::{
    AssignedConsumerRecvRegistration, AssignedConsumerRecvSignal, AssignedConsumerRecvWait,
};
pub(crate) use ticket::AssignedConsumerRecvTicket;
