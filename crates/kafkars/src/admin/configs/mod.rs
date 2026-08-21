//! Declarative facade for topic convenience and generic resource `DescribeConfigs`.

mod builder;
mod entry;
mod operation;
mod query;
mod resource_builder;
mod resource_operation;
mod resource_query;
mod resource_result;
mod result;

pub use builder::DescribeConfigsBuilder;
pub use entry::{ConfigEntry, ConfigSynonym};
pub use operation::DescribeConfigs;
pub use query::TopicConfigQuery;
pub use resource_builder::DescribeConfigResourcesBuilder;
pub use resource_operation::DescribeConfigResources;
pub use resource_query::ConfigResourceQuery;
pub use resource_result::DescribeConfigResourcesResult;
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
mod resource_builder_test;
#[cfg(test)]
mod resource_operation_test;
#[cfg(test)]
mod resource_query_test;
#[cfg(test)]
mod resource_result_test;
#[cfg(test)]
mod result_test;
