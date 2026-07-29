//! Admission handoff for public Admin `ListConfigResources`.

use std::time::Duration;

use super::AdminEngine;
use crate::{admin::ConfigResourceType, bridge::list_config_resources::AdminListConfigResources};

impl AdminEngine {
    pub(crate) fn submit_list_config_resources(
        &self,
        resource_types: Vec<ConfigResourceType>,
        timeout: Duration,
    ) -> AdminListConfigResources {
        let resource_types = resource_types
            .into_iter()
            .map(ConfigResourceType::as_raw)
            .collect();
        AdminListConfigResources::from_admission(
            self.handle
                .try_list_config_resources(resource_types, timeout),
        )
    }
}
