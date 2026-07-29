//! Public generic legacy replacement result scenarios.

use std::time::Duration;

use crate::admin::{
    BatchResult, ConfigResource, ConfigResourceType,
    legacy_replace_topic_configs::LegacyReplaceConfigResourcesResult,
};

#[test]
fn result_preserves_throttle_and_exact_resource_order() {
    let result = LegacyReplaceConfigResourcesResult::new(
        Duration::from_millis(29),
        BatchResult::new(vec![
            (
                ConfigResource::new(ConfigResourceType::Broker, "7".to_owned()),
                Ok(()),
            ),
            (
                ConfigResource::new(ConfigResourceType::BrokerLogger, "7".to_owned()),
                Ok(()),
            ),
        ]),
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(29));
    assert_eq!(
        result
            .resources()
            .entries()
            .iter()
            .map(|(resource, _result)| (resource.resource_type(), resource.name()))
            .collect::<Vec<_>>(),
        [
            (ConfigResourceType::Broker, "7"),
            (ConfigResourceType::BrokerLogger, "7"),
        ]
    );
}
