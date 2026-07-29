//! Declarative facade for destructive topic and generic legacy replacement.

mod builder;
mod entry;
mod operation;
mod replacement;
mod resource_builder;
mod resource_operation;
mod resource_replacement;
mod resource_result;
mod result;

pub use builder::LegacyReplaceTopicConfigsBuilder;
pub use entry::LegacyTopicConfigEntry;
pub use operation::LegacyReplaceTopicConfigs;
pub use replacement::LegacyTopicConfigReplacement;
pub use resource_builder::LegacyReplaceConfigResourcesBuilder;
pub use resource_operation::LegacyReplaceConfigResources;
pub use resource_replacement::LegacyConfigResourceReplacement;
pub use resource_result::LegacyReplaceConfigResourcesResult;
pub use result::LegacyReplaceTopicConfigsResult;

#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod entry_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod replacement_test;
#[cfg(test)]
mod resource_builder_test;
#[cfg(test)]
mod resource_operation_test;
#[cfg(test)]
mod resource_replacement_test;
#[cfg(test)]
mod resource_result_test;
#[cfg(test)]
mod result_test;
