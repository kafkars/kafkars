//! Declarative private boundary between the Rust facade and shared engine.

pub(crate) mod admin;
pub(crate) mod admin_configs_operation;
pub(crate) mod admin_configs_request;
pub(crate) mod admin_configs_result;
pub(crate) mod admin_delete_operation;
pub(crate) mod admin_delete_result;
pub(crate) mod admin_describe_operation;
pub(crate) mod admin_describe_result;
pub(crate) mod admin_operation;
pub(crate) mod admin_partitions_operation;
pub(crate) mod admin_partitions_result;
pub(crate) mod admin_result;
pub(crate) mod admin_topics_operation;
pub(crate) mod admin_topics_result;
mod client;
pub(crate) mod consumer;
pub(crate) mod producer;
pub(crate) mod producer_barrier;
pub(crate) mod producer_delivery;
pub(crate) mod producer_result;
pub(crate) use client::ClientEngine;
#[cfg(test)]
mod admin_configs_operation_test;
#[cfg(test)]
mod admin_configs_request_test;
#[cfg(test)]
mod admin_configs_result_test;
#[cfg(test)]
mod admin_delete_operation_test;
#[cfg(test)]
mod admin_delete_result_test;
#[cfg(test)]
mod admin_describe_operation_test;
#[cfg(test)]
mod admin_describe_result_test;
#[cfg(test)]
mod admin_operation_test;
#[cfg(test)]
mod admin_partitions_operation_test;
#[cfg(test)]
mod admin_partitions_result_test;
#[cfg(test)]
mod admin_result_test;
#[cfg(test)]
mod admin_test;
#[cfg(test)]
mod admin_topics_operation_test;
#[cfg(test)]
mod admin_topics_result_test;
#[cfg(test)]
mod client_test;
#[cfg(test)]
mod producer_barrier_test;
#[cfg(test)]
mod producer_delivery_test;
#[cfg(test)]
mod producer_test;
