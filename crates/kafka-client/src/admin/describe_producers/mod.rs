//! Declarative facade for public Admin `DescribeProducers` values and operation.

mod builder;
mod operation;
mod result;
mod value;

pub use builder::DescribeProducersBuilder;
pub use operation::DescribeProducers;
pub use result::DescribeProducersResult;
pub use value::ProducerState;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod result_test;
#[cfg(test)]
mod value_test;
