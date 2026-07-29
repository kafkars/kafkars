//! Declarative private bridge for Kafka feature discovery.

mod engine;
mod operation;
mod result;

pub(crate) use operation::AdminDescribeFeatures;

#[cfg(test)]
mod result_test;
