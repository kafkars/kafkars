//! Stable broker-unregistration builder, operation, and result facade.

mod builder;
mod operation;
mod result;

pub use builder::UnregisterBrokerBuilder;
pub use operation::UnregisterBroker;
pub use result::UnregisterBrokerResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
