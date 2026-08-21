//! Declarative public facade for caller-anchored transactional producer fencing.

mod builder;
mod operation;
mod result;

pub use builder::FenceProducersBuilder;
pub use operation::FenceProducers;
pub use result::{FenceProducersResult, FencedProducerIdentity};

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
