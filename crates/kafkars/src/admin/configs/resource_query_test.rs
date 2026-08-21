//! Public generic configuration-resource query scenarios.

use crate::admin::{ConfigResourceType, configs::ConfigResourceQuery};

#[test]
fn positive_known_and_future_resource_types_remain_exact() {
    for (resource_type, name) in [
        (ConfigResourceType::Broker, "7"),
        (ConfigResourceType::Group, "orders-workers"),
        (ConfigResourceType::ClientMetrics, "telemetry"),
        (ConfigResourceType::from_raw(64), "future-resource"),
    ] {
        let query =
            ConfigResourceQuery::new(resource_type, name).configuration_keys(["one", "two"]);
        assert_eq!(query.resource_type(), resource_type);
        assert_eq!(query.resource_name(), name);
        assert_eq!(
            query.selected_configuration_keys(),
            Some(["one".to_owned(), "two".to_owned()].as_slice())
        );
    }
}

#[test]
fn nonpositive_types_remain_inert_for_submission_validation() {
    for value in [0, -1, i8::MIN] {
        let query = ConfigResourceQuery::new(ConfigResourceType::from_raw(value), "invalid");
        assert_eq!(query.resource_type().as_raw(), value);
    }
}
