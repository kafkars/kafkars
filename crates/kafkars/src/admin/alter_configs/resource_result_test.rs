//! Public generic incremental configuration result scenarios.

use std::time::Duration;

use crate::admin::{
    BatchResult, ConfigResource, ConfigResourceType,
    alter_configs::IncrementalAlterConfigResourcesResult,
};

#[test]
fn result_preserves_throttle_and_resource_order() {
    let result = IncrementalAlterConfigResourcesResult::new(
        Duration::from_millis(19),
        BatchResult::new(vec![
            (
                ConfigResource::new(ConfigResourceType::Broker, "7".to_owned()),
                Ok(()),
            ),
            (
                ConfigResource::new(ConfigResourceType::Group, "orders-workers".to_owned()),
                Ok(()),
            ),
        ]),
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(19));
    assert_eq!(
        result
            .resources()
            .entries()
            .iter()
            .map(|(resource, _result)| (resource.resource_type(), resource.name()))
            .collect::<Vec<_>>(),
        [
            (ConfigResourceType::Broker, "7"),
            (ConfigResourceType::Group, "orders-workers"),
        ]
    );
}
