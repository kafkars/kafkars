//! Declarative facade for batched `CreateTopics` construction and observation.

mod batch_result;
mod builder;
mod create_topics;
mod delete_builder;
mod delete_topics;
mod handle;
mod new_topic;

pub use batch_result::BatchResult;
pub use builder::CreateTopicsBuilder;
pub use create_topics::CreateTopics;
pub use delete_builder::DeleteTopicsBuilder;
pub use delete_topics::DeleteTopics;
pub use handle::Admin;
pub use new_topic::NewTopic;

#[cfg(test)]
mod batch_result_test;
#[cfg(test)]
mod builder_test;
#[cfg(test)]
mod create_topics_test;
#[cfg(test)]
mod delete_builder_test;
#[cfg(test)]
mod delete_topics_test;
#[cfg(test)]
mod handle_test;
#[cfg(test)]
mod new_topic_test;
