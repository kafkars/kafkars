//! Concrete bounded admin owners without a generic state-machine framework.
mod completion;
#[cfg(test)]
mod completion_test;
mod configs;
mod delete_error;
mod delete_handle;
mod delete_host;
mod delete_model;
mod delete_observer;
mod delete_outcome;
mod delete_shard;
mod describe_error;
mod describe_handle;
mod describe_host;
mod describe_observer;
mod describe_outcome;
mod describe_shard;
mod error;
mod exports;
mod handle;
mod host;
mod model;
mod observer;
mod outcome;
mod partitions_error;
mod partitions_handle;
mod partitions_host;
mod partitions_model;
mod partitions_observer;
mod partitions_outcome;
mod partitions_shard;
mod public_api;
pub(crate) mod retention;
mod shard;
#[cfg(test)]
mod test_support;
mod topics_error;
mod topics_handle;
mod topics_host;
mod topics_model;
mod topics_observer;
mod topics_outcome;
mod topics_shard;
pub(crate) use exports::*;
pub use public_api::*;
#[cfg(test)]
mod delete_handle_test;
#[cfg(test)]
mod delete_host_test;
#[cfg(test)]
mod delete_model_test;
#[cfg(test)]
mod delete_observer_test;
#[cfg(test)]
mod delete_shard_test;
#[cfg(test)]
mod describe_handle_test;
#[cfg(test)]
mod describe_host_test;
#[cfg(test)]
mod describe_observer_test;
#[cfg(test)]
mod describe_outcome_test;
#[cfg(test)]
mod describe_shard_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod partitions_handle_test;
#[cfg(test)]
mod partitions_host_test;
#[cfg(test)]
mod partitions_model_test;
#[cfg(test)]
mod partitions_observer_test;
#[cfg(test)]
mod partitions_shard_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod shard_test;
#[cfg(test)]
mod topics_host_test;
#[cfg(test)]
mod topics_model_test;
#[cfg(test)]
mod topics_outcome_test;
#[cfg(test)]
mod topics_shard_test;
