//! Declarative facade for topic-scoped `IncrementalAlterConfigs`.

mod alteration;
mod builder;
mod operation;
mod result;
mod topic;

pub use alteration::{ConfigAlteration, ConfigAlterationOperation};
pub use builder::IncrementalAlterConfigsBuilder;
pub use operation::IncrementalAlterConfigs;
pub use result::IncrementalAlterConfigsResult;
pub use topic::TopicConfigAlterations;

#[cfg(test)]
mod alteration_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod result_test;
#[cfg(test)]
mod topic_test;
