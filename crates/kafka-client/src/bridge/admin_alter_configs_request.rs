//! Prebuilt `IncrementalAlterConfigs` request retained by inert facade builders.

use kafka_client_engine::{
    IncrementalAlterConfigsRequest as EngineRequest,
    IncrementalConfigAlteration as EngineAlteration, IncrementalConfigOperation as EngineOperation,
    IncrementalConfigResourceAlterations as EngineResourceAlterations,
    TopicConfigAlterations as EngineTopicAlterations,
};

use crate::admin::{
    ConfigAlteration, ConfigAlterationOperation, ConfigResourceAlterations, TopicConfigAlterations,
};

/// Engine request prepared before the public submission boundary.
pub(crate) struct IncrementalAlterConfigsAdminRequest {
    inner: EngineRequest,
}

impl IncrementalAlterConfigsAdminRequest {
    pub(crate) fn from_topics<I>(topics: I) -> Self
    where
        I: IntoIterator<Item = TopicConfigAlterations>,
    {
        Self {
            inner: EngineRequest::new(topics.into_iter().map(into_engine_topic).collect()),
        }
    }

    pub(crate) fn from_resources<I>(resources: I) -> Self
    where
        I: IntoIterator<Item = ConfigResourceAlterations>,
    {
        Self {
            inner: EngineRequest::for_resources(
                resources.into_iter().map(into_engine_resource).collect(),
            ),
        }
    }

    pub(crate) fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.inner = self.inner.with_validate_only(validate_only);
        self
    }

    pub(crate) fn into_engine(self) -> EngineRequest {
        self.inner
    }
}

impl std::fmt::Debug for IncrementalAlterConfigsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IncrementalAlterConfigsAdminRequest")
            .finish_non_exhaustive()
    }
}

fn into_engine_topic(topic: TopicConfigAlterations) -> EngineTopicAlterations {
    let (topic, alterations) = topic.into_parts();
    EngineTopicAlterations::new(
        topic,
        alterations
            .into_iter()
            .map(into_engine_alteration)
            .collect(),
    )
}

fn into_engine_resource(resource: ConfigResourceAlterations) -> EngineResourceAlterations {
    let (resource_type, resource_name, alterations) = resource.into_parts();
    EngineResourceAlterations::resource(
        resource_type.as_raw(),
        resource_name,
        alterations
            .into_iter()
            .map(into_engine_alteration)
            .collect(),
    )
}

fn into_engine_alteration(alteration: ConfigAlteration) -> EngineAlteration {
    let (key, operation) = alteration.into_parts();
    EngineAlteration::new(key, into_engine_operation(operation))
}

fn into_engine_operation(operation: ConfigAlterationOperation) -> EngineOperation {
    match operation {
        ConfigAlterationOperation::Set(value) => EngineOperation::Set(value),
        ConfigAlterationOperation::Delete => EngineOperation::Delete,
        ConfigAlterationOperation::Append(value) => EngineOperation::Append(value),
        ConfigAlterationOperation::Subtract(value) => EngineOperation::Subtract(value),
    }
}
