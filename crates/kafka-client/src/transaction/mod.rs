//! Declarative public transaction-initialization surface.

mod builder;
mod identity;
mod initialization;
mod producer;

pub use builder::TransactionalProducerBuilder;
pub use identity::TransactionalProducerIdentity;
pub use initialization::InitializeTransactionalProducer;
pub use producer::TransactionalProducer;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod initialization_test;
#[cfg(test)]
mod producer_test;
