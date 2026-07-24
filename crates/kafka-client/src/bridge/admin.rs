//! Sole conversion and admission boundary from public admin values to the engine.

mod describe_cluster_submit;

use std::time::Duration;

use kafka_client_engine::{
    AdminHandle as EngineAdminHandle, CreatePartitionsRequest as EnginePartitionsRequest,
    CreateTopic as EngineTopic, CreateTopicConfig as EngineTopicConfig,
    CreateTopicsRequest as EngineRequest, DeleteTopicsRequest as EngineDeleteRequest,
    DescribeTopicsRequest as EngineDescribeTopicsRequest,
    PartitionIncrease as EnginePartitionIncrease,
};

use crate::admin::{NewPartitions, NewTopic};

use super::admin_delete_operation::AdminDeleteTopics;
use super::admin_operation::AdminCreateTopics;
use super::admin_partitions_operation::AdminCreatePartitions;
use super::admin_topics_operation::AdminDescribeTopics;

/// Cloneable facade owner of the engine's concrete admin handle and default.
#[derive(Debug, Clone)]
pub(crate) struct AdminEngine {
    handle: EngineAdminHandle,
    default_timeout: Duration,
}

impl AdminEngine {
    pub(crate) const fn new(handle: EngineAdminHandle, default_timeout: Duration) -> Self {
        Self {
            handle,
            default_timeout,
        }
    }

    pub(crate) const fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    pub(crate) fn submit(&self, request: AdminRequest, timeout: Duration) -> AdminCreateTopics {
        AdminCreateTopics::from_admission(self.handle.try_create_topics(request.inner, timeout))
    }

    pub(crate) fn submit_delete(
        &self,
        request: DeleteAdminRequest,
        timeout: Duration,
    ) -> AdminDeleteTopics {
        AdminDeleteTopics::from_admission(self.handle.try_delete_topics(request.inner, timeout))
    }
    pub(crate) fn submit_describe_topics(
        &self,
        request: DescribeTopicsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTopics {
        AdminDescribeTopics::from_admission(self.handle.try_describe_topics(request.inner, timeout))
    }

    pub(crate) fn submit_create_partitions(
        &self,
        request: PartitionsAdminRequest,
        timeout: Duration,
    ) -> AdminCreatePartitions {
        AdminCreatePartitions::from_admission(
            self.handle.try_create_partitions(request.inner, timeout),
        )
    }
}

/// Prepared engine request retained by an inert topic-description builder.
pub(crate) struct DescribeTopicsAdminRequest {
    inner: EngineDescribeTopicsRequest,
}

impl DescribeTopicsAdminRequest {
    pub(crate) fn from_topics<I, T>(topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            inner: EngineDescribeTopicsRequest::new(topics.into_iter().map(Into::into).collect()),
        }
    }
}

impl std::fmt::Debug for DescribeTopicsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeTopicsAdminRequest")
            .finish_non_exhaustive()
    }
}

/// Prepared engine request retained by an inert partition builder.
pub(crate) struct PartitionsAdminRequest {
    inner: EnginePartitionsRequest,
}

impl PartitionsAdminRequest {
    pub(crate) fn from_topics<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = NewPartitions>,
    {
        Self {
            inner: EnginePartitionsRequest::new(
                topics.into_iter().map(into_engine_partitions).collect(),
            ),
        }
    }

    pub(crate) fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.inner = self.inner.with_validate_only(validate_only);
        self
    }
}

impl std::fmt::Debug for PartitionsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartitionsAdminRequest")
            .finish_non_exhaustive()
    }
}

/// Prepared engine request retained by an inert deletion builder.
pub(crate) struct DeleteAdminRequest {
    inner: EngineDeleteRequest,
}

impl DeleteAdminRequest {
    pub(crate) fn from_topics<I, T>(topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            inner: EngineDeleteRequest::new(topics.into_iter().map(Into::into).collect()),
        }
    }
}

impl std::fmt::Debug for DeleteAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeleteAdminRequest")
            .finish_non_exhaustive()
    }
}

/// Prepared engine request retained by an otherwise inert public builder.
pub(crate) struct AdminRequest {
    inner: EngineRequest,
}

impl AdminRequest {
    pub(crate) fn from_topics<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = NewTopic>,
    {
        Self {
            inner: EngineRequest::new(topics.into_iter().map(into_engine_topic).collect()),
        }
    }

    pub(crate) fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.inner = self.inner.with_validate_only(validate_only);
        self
    }
}

impl std::fmt::Debug for AdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminRequest")
            .finish_non_exhaustive()
    }
}

fn into_engine_topic(topic: NewTopic) -> EngineTopic {
    let (name, partitions, replication_factor, configs) = topic.into_parts();
    configs.into_iter().fold(
        EngineTopic::new(name, partitions).with_replication_factor(replication_factor),
        |topic, (name, value)| topic.with_config(EngineTopicConfig::new(name, Some(value))),
    )
}

fn into_engine_partitions(topic: NewPartitions) -> EnginePartitionIncrease {
    let (name, total_count) = topic.into_parts();
    EnginePartitionIncrease::new(name, total_count)
}
