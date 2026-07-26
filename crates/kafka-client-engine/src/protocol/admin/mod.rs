//! Generated-message adaptation for concrete Kafka admin operations.

pub(crate) mod create_partitions;
mod create_partitions_budget;
pub(crate) mod create_topics;
pub(crate) mod delete_topics;
mod delete_topics_budget;
pub(crate) mod describe_cluster;
pub(crate) mod describe_configs;
mod describe_configs_budget;
mod describe_configs_model;
pub(crate) mod describe_configs_response;
mod describe_configs_values;
mod describe_topic_value;
pub(crate) mod describe_topics;
mod describe_topics_budget;
pub(crate) mod describe_topics_response;
pub(crate) mod incremental_alter_configs;
mod list_topics_response;
mod request_timeout_error;
#[cfg(test)]
mod request_timeout_error_test;
mod result_budget;

#[cfg(test)]
mod create_partitions_budget_test;
#[cfg(test)]
mod create_partitions_test;
#[cfg(test)]
mod create_topics_test;
#[cfg(test)]
mod delete_topics_budget_test;
#[cfg(test)]
mod delete_topics_test;
#[cfg(test)]
mod describe_cluster_test;
#[cfg(test)]
mod describe_configs_budget_test;
#[cfg(test)]
mod describe_configs_model_test;
#[cfg(test)]
mod describe_configs_response_test;
#[cfg(test)]
mod describe_configs_test;
#[cfg(test)]
mod describe_configs_values_test;
#[cfg(test)]
mod describe_topic_value_test;
#[cfg(test)]
mod describe_topics_budget_test;
#[cfg(test)]
mod describe_topics_response_test;
#[cfg(test)]
mod describe_topics_test;
#[cfg(test)]
mod list_topics_response_test;
#[cfg(test)]
mod result_budget_test;
