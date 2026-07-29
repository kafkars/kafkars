//! Explicit legacy full-snapshot topic configuration replacement entrypoint.

use super::Admin;
use crate::{
    admin::{
        LegacyConfigResourceReplacement, LegacyReplaceConfigResourcesBuilder,
        LegacyReplaceTopicConfigsBuilder, LegacyTopicConfigReplacement,
    },
    bridge::legacy_replace_topic_configs::LegacyReplaceTopicConfigsAdminRequest,
};

impl Admin {
    /// Builds inert complete configuration snapshots for legacy Kafka API 33.
    ///
    /// This is destructive replacement, not incremental alteration: omitted
    /// keys are reset, nullable entry values reset their named keys, and an
    /// empty per-topic snapshot remains meaningful.
    ///
    /// Topic and key validation remains deferred until
    /// [`LegacyReplaceTopicConfigsBuilder::submit`] captures the public
    /// absolute deadline and attempts bounded engine admission.
    pub fn legacy_replace_topic_configs<I>(
        &self,
        replacements: I,
    ) -> LegacyReplaceTopicConfigsBuilder
    where
        I: IntoIterator<Item = LegacyTopicConfigReplacement>,
    {
        let request =
            LegacyReplaceTopicConfigsAdminRequest::new(replacements.into_iter().collect());
        LegacyReplaceTopicConfigsBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }

    /// Builds inert destructive snapshots for generic legacy Kafka API 33.
    ///
    /// This replaces complete snapshots without API 44 fallback or retry.
    /// Omitted keys are reset and an empty snapshot remains destructive.
    /// Validation and deadline capture occur only at
    /// [`LegacyReplaceConfigResourcesBuilder::submit`].
    pub fn legacy_replace_config_resources<I>(
        &self,
        replacements: I,
    ) -> LegacyReplaceConfigResourcesBuilder
    where
        I: IntoIterator<Item = LegacyConfigResourceReplacement>,
    {
        let request = LegacyReplaceTopicConfigsAdminRequest::for_resources(
            replacements.into_iter().collect(),
        );
        LegacyReplaceConfigResourcesBuilder::new(
            self.engine.clone(),
            request,
            self.engine.default_timeout(),
        )
    }
}
