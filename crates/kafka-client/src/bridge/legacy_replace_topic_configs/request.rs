//! Public legacy full-snapshot intent translated after deadline capture.

use crate::admin::{
    LegacyConfigResourceReplacement, LegacyTopicConfigEntry, LegacyTopicConfigReplacement,
};

use super::engine::{
    Entry as EngineEntry, Request as EngineRequest, ResourceReplacement, TopicReplacement,
};

enum Selection {
    Topics(Vec<LegacyTopicConfigReplacement>),
    Resources(Vec<LegacyConfigResourceReplacement>),
}

/// Request retained by the inert public builder before submission.
pub(crate) struct LegacyReplaceTopicConfigsAdminRequest {
    selection: Selection,
    validate_only: bool,
}

impl LegacyReplaceTopicConfigsAdminRequest {
    pub(crate) const fn new(replacements: Vec<LegacyTopicConfigReplacement>) -> Self {
        Self {
            selection: Selection::Topics(replacements),
            validate_only: false,
        }
    }

    pub(crate) const fn for_resources(replacements: Vec<LegacyConfigResourceReplacement>) -> Self {
        Self {
            selection: Selection::Resources(replacements),
            validate_only: false,
        }
    }

    pub(crate) const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        match self.selection {
            Selection::Topics(replacements) => EngineRequest::new(
                replacements
                    .into_iter()
                    .map(into_engine_topic_replacement)
                    .collect(),
            ),
            Selection::Resources(replacements) => EngineRequest::for_resources(
                replacements
                    .into_iter()
                    .map(into_engine_resource_replacement)
                    .collect(),
            ),
        }
        .with_validate_only(self.validate_only)
    }
}

impl std::fmt::Debug for LegacyReplaceTopicConfigsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LegacyReplaceTopicConfigsAdminRequest")
            .field("selection", &selection_name(&self.selection))
            .field("validate_only", &self.validate_only)
            .finish()
    }
}

fn selection_name(selection: &Selection) -> &'static str {
    match selection {
        Selection::Topics(_) => "topics",
        Selection::Resources(_) => "resources",
    }
}

fn into_engine_topic_replacement(replacement: LegacyTopicConfigReplacement) -> TopicReplacement {
    let (topic, entries) = replacement.into_parts();
    TopicReplacement::new(topic, entries.into_iter().map(into_engine_entry).collect())
}

fn into_engine_resource_replacement(
    replacement: LegacyConfigResourceReplacement,
) -> ResourceReplacement {
    let (resource_type, resource_name, entries) = replacement.into_parts();
    ResourceReplacement::resource(
        resource_type.as_raw(),
        resource_name,
        entries.into_iter().map(into_engine_entry).collect(),
    )
}

fn into_engine_entry(entry: LegacyTopicConfigEntry) -> EngineEntry {
    let (key, value) = entry.into_parts();
    EngineEntry::new(key, value)
}
