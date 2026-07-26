//! Runtime-neutral waiting for one retained assigned-consumer failure event.

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

pub use error::{AssignedConsumerNextEventError, AssignedConsumerNextEventErrorKind};
pub use operation::AssignedConsumerNextEvent;
pub(crate) use signal::{
    AssignedConsumerEventRegistration, AssignedConsumerEventSignal, AssignedConsumerEventWait,
};
pub(crate) use ticket::AssignedConsumerEventTicket;
