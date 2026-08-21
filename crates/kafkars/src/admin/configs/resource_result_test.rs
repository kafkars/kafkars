//! Public generic configuration-result ownership scenarios.

use std::time::Duration;

use crate::admin::{
    BatchResult, ConfigResource, ConfigResourceType, configs::DescribeConfigResourcesResult,
};

#[test]
fn result_preserves_throttle_and_resource_identity_order() {
    let result = DescribeConfigResourcesResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            (
                ConfigResource::new(ConfigResourceType::Broker, "7".to_owned()),
                Ok(Vec::new()),
            ),
            (
                ConfigResource::new(ConfigResourceType::Group, "workers".to_owned()),
                Ok(Vec::new()),
            ),
        ]),
    );
    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(
        result
            .resources()
            .entries()
            .iter()
            .map(|(resource, _result)| (resource.resource_type(), resource.name()))
            .collect::<Vec<_>>(),
        [
            (ConfigResourceType::Broker, "7"),
            (ConfigResourceType::Group, "workers"),
        ]
    );
}
