//! Scenarios for allocation of validated normalized configuration values.

use kafka_wire::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
};

use super::describe_configs::DescribeConfigsQuery;
use super::describe_configs_values::normalize_resource;

#[test]
fn value_allocation_canonicalizes_config_and_synonym_order() {
    let query = DescribeConfigsQuery {
        resource_type: 2,
        resource_name: "orders",
        configuration_keys: None,
    };
    let mut zeta = config("zeta");
    zeta.synonyms = vec![synonym("later", 5), synonym("earlier", -1)];
    let alpha = config("alpha");
    let mut result = DescribeConfigsResult::default();
    result.resource_type = 2;
    result.resource_name = "orders".into();
    result.error_code = 0;
    result.error_message = None;
    result.configs = vec![zeta, alpha];

    let Ok(normalized) = normalize_resource(&query, &result, 4) else {
        panic!("validated fixture must allocate");
    };
    let Ok(configs) = normalized.outcome else {
        panic!("successful resource must stay successful");
    };
    assert_eq!(configs[0].name, "alpha");
    assert_eq!(configs[1].name, "zeta");
    assert_eq!(configs[1].synonyms[0].name, "earlier");
    assert_eq!(configs[1].synonyms[0].source, -1);
}

fn config(name: &str) -> DescribeConfigsResourceResult {
    let mut config = DescribeConfigsResourceResult::default();
    config.name = name.into();
    config.value = None;
    config.documentation = None;
    config
}

fn synonym(name: &str, source: i8) -> DescribeConfigsSynonym {
    let mut synonym = DescribeConfigsSynonym::default();
    synonym.name = name.into();
    synonym.value = None;
    synonym.source = source;
    synonym
}
