//! Declarative private bridge for metadata-quorum descriptions.

mod engine;
mod operation;
mod result;

pub(crate) use operation::AdminDescribeMetadataQuorum;

#[cfg(test)]
mod result_test;
