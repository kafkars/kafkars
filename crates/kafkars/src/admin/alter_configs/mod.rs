//! Declarative facade for topic convenience and generic `IncrementalAlterConfigs`.

mod alteration;
mod builder;
mod operation;
mod resource;
mod resource_builder;
mod resource_operation;
mod resource_result;
mod result;
mod topic;

pub use alteration::{ConfigAlteration, ConfigAlterationOperation};
pub use builder::IncrementalAlterConfigsBuilder;
pub use operation::IncrementalAlterConfigs;
pub use resource::ConfigResourceAlterations;
pub use resource_builder::IncrementalAlterConfigResourcesBuilder;
pub use resource_operation::IncrementalAlterConfigResources;
pub use resource_result::IncrementalAlterConfigResourcesResult;
pub use result::IncrementalAlterConfigsResult;
pub use topic::TopicConfigAlterations;

#[cfg(test)]
mod alteration_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod resource_builder_test;
#[cfg(test)]
mod resource_operation_test;
#[cfg(test)]
mod resource_result_test;
#[cfg(test)]
mod resource_test;
#[cfg(test)]
mod result_test;
#[cfg(test)]
mod topic_test;
