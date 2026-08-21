//! Declarative private bridge for Kafka broker unregistration.

mod engine;
mod operation;
mod result;

pub(crate) use operation::AdminUnregisterBroker;

#[cfg(test)]
mod result_test;
