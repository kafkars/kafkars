//! Sole conversion and admission boundary from public admin values to the engine.

mod describe_cluster_submit;

use std::time::Duration;

use kafka_client_engine::{
    AdminHandle as EngineAdminHandle, CreateTopic as EngineTopic,
    CreateTopicConfig as EngineTopicConfig, CreateTopicsRequest as EngineRequest,
    DeleteTopicsRequest as EngineDeleteRequest,
};

use crate::admin::NewTopic;

use super::admin_delete_operation::AdminDeleteTopics;
use super::admin_operation::AdminCreateTopics;

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
