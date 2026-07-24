//! Generated-message adaptation for concrete Kafka admin operations.

pub(crate) mod create_partitions;
mod create_partitions_budget;
pub(crate) mod create_topics;
pub(crate) mod delete_topics;
mod delete_topics_budget;
pub(crate) mod describe_cluster;
pub(crate) mod describe_topics;
mod describe_topics_budget;
pub(crate) mod describe_topics_response;
mod result_budget;
mod timeout;

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
mod describe_topics_budget_test;
#[cfg(test)]
mod describe_topics_response_test;
#[cfg(test)]
mod describe_topics_test;
#[cfg(test)]
mod result_budget_test;
#[cfg(test)]
mod timeout_test;
