//! Scenarios for generated `DescribeConfigs` request construction.

use super::describe_configs::{DescribeConfigsQuery, describe_configs_request};

#[test]
fn request_preserves_resource_and_key_order_without_policy_expansion() {
    let selected_keys = ["cleanup.policy", "retention.ms"];
    let queries = [
        DescribeConfigsQuery {
            resource_type: 2,
            resource_name: "orders",
            configuration_keys: Some(&selected_keys),
        },
        DescribeConfigsQuery {
            resource_type: 4,
            resource_name: "7",
            configuration_keys: None,
        },
    ];

    let request = describe_configs_request(&queries, true, true);

    assert_eq!(request.resources.len(), 2);
    assert_eq!(request.resources[0].resource_type, 2);
    assert_eq!(request.resources[0].resource_name.as_str(), "orders");
    let Some(keys) = &request.resources[0].configuration_keys else {
        panic!("selected configuration keys must remain non-null");
    };
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_str(), selected_keys[0]);
    assert_eq!(keys[1].as_str(), selected_keys[1]);
    assert_eq!(request.resources[1].resource_type, 4);
    assert_eq!(request.resources[1].resource_name.as_str(), "7");
    assert_eq!(request.resources[1].configuration_keys, None);
    assert!(request.include_synonyms);
    assert!(request.include_documentation);
}
