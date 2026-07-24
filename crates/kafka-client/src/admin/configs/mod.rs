//! Declarative facade for topic-scoped `DescribeConfigs`.

mod builder;
mod entry;
mod operation;
mod query;
mod result;

pub use builder::DescribeConfigsBuilder;
pub use entry::{ConfigEntry, ConfigSynonym};
pub use operation::DescribeConfigs;
pub use query::TopicConfigQuery;
pub use result::DescribeConfigsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod entry_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod query_test;
#[cfg(test)]
mod result_test;
