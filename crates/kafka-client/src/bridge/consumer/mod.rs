//! Declarative private boundary for assigned-consumer facade translation.

mod assignment;
mod assignment_result;
mod batch;
mod batch_result;
mod close;
mod control;
mod control_result;
mod event;
mod event_result;
mod handle;
mod next_event;
mod next_event_result;
mod recv;
mod recv_result;
mod result;

pub(crate) use batch::{
    AssignedConsumerBatch, AssignedConsumerHeader, AssignedConsumerRecord, AssignedConsumerRecords,
};
pub(crate) use close::AssignedConsumerClose;
pub(crate) use handle::AssignedConsumerEngine;
pub(crate) use next_event::AssignedConsumerNextEvent;
pub(crate) use recv::AssignedConsumerRecv;

#[cfg(test)]
mod assignment_result_test;
#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod batch_result_test;
#[cfg(test)]
mod batch_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod control_result_test;
#[cfg(test)]
mod control_test;
#[cfg(test)]
mod event_result_test;
#[cfg(test)]
mod event_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod next_event_result_test;
#[cfg(test)]
mod next_event_test;
#[cfg(test)]
mod recv_result_test;
#[cfg(test)]
mod recv_test;
#[cfg(test)]
mod result_test;
