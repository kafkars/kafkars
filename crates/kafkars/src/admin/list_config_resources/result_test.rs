//! Stable generated-free configuration-resource result tests.

use std::time::Duration;

use super::{ConfigResource, ConfigResourceType, ListConfigResourcesResult};

#[test]
fn result_preserves_throttle_and_canonical_resources() {
    let broker = ConfigResource::new(ConfigResourceType::Broker, "1".to_owned());
    let topic = ConfigResource::new(ConfigResourceType::Topic, "orders".to_owned());
    let result = ListConfigResourcesResult::new(
        Duration::from_millis(19),
        vec![topic.clone(), broker.clone()],
    );

    assert_eq!(result.throttle(), Duration::from_millis(19));
    assert_eq!(result.resources(), [topic.clone(), broker.clone()]);
    assert_eq!(
        result.into_parts(),
        (Duration::from_millis(19), vec![topic, broker])
    );
}
