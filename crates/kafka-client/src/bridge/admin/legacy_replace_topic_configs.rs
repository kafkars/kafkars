//! Capture-first facade admission for legacy full-snapshot replacement.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::legacy_replace_topic_configs::{
    AdminLegacyReplaceConfigResources, AdminLegacyReplaceTopicConfigs,
    LegacyReplaceTopicConfigsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_legacy_replace_topic_configs(
        &self,
        request: LegacyReplaceTopicConfigsAdminRequest,
        timeout: Duration,
    ) -> AdminLegacyReplaceTopicConfigs {
        let admission = match self.handle.capture_legacy_alter_configs(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminLegacyReplaceTopicConfigs::from_admission(admission)
    }

    pub(crate) fn submit_legacy_replace_config_resources(
        &self,
        request: LegacyReplaceTopicConfigsAdminRequest,
        timeout: Duration,
    ) -> AdminLegacyReplaceConfigResources {
        let admission = match self.handle.capture_legacy_alter_configs(timeout) {
            Ok(capture) => capture.try_submit(request.into_engine()),
            Err(error) => Err(error),
        };
        AdminLegacyReplaceConfigResources::from_admission(admission)
    }
}
