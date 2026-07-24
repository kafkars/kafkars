//! Scenarios for exact normalized `DescribeConfigs` retained-byte admission.

use kafka_wire::{
    DescribeConfigsResponse,
    describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    },
};

use super::describe_configs::DescribeConfigsQuery;
use super::describe_configs_budget::required_retained_bytes;
use super::describe_configs_response::{
    DescribeConfigsProtocolFailure, normalize_describe_configs_response,
};

const API_VERSION: i16 = 4;

#[test]
fn exact_normalized_charge_is_accepted_and_one_byte_less_is_rejected() {
    let keys = ["cleanup.policy"];
    let queries = [DescribeConfigsQuery {
        resource_type: 2,
        resource_name: "orders",
        configuration_keys: Some(&keys),
    }];
    let mut config = DescribeConfigsResourceResult::default();
    config.name = "cleanup.policy".into();
    config.value = Some("compact".into());
    config.documentation = Some("documentation".into());
    let mut synonym = DescribeConfigsSynonym::default();
    synonym.name = "log.cleanup.policy".into();
    synonym.value = Some("delete".into());
    synonym.source = 5;
    config.synonyms = vec![synonym];
    let response = successful_response(config);
    let Ok(required) = required_retained_bytes(&queries, &response, API_VERSION) else {
        panic!("fixture charge must be representable");
    };

    assert!(
        normalize_describe_configs_response(&queries, &response, API_VERSION, required,).is_ok()
    );
    assert_eq!(
        normalize_describe_configs_response(
            &queries,
            &response,
            API_VERSION,
            required.saturating_sub(1),
        ),
        Err(DescribeConfigsProtocolFailure::RetainedBytes)
    );
}

#[test]
fn hostile_diagnostic_is_utf8_bounded_before_terminal_copy() {
    let queries = [DescribeConfigsQuery {
        resource_type: 2,
        resource_name: "orders",
        configuration_keys: None,
    }];
    let diagnostic = "é".repeat(700);
    let mut result = DescribeConfigsResult::default();
    result.resource_type = 2;
    result.resource_name = "orders".into();
    result.error_code = -32_123;
    result.error_message = Some(diagnostic.as_str().into());
    result.configs = Vec::new();
    let mut response = DescribeConfigsResponse::default();
    response.results = vec![result];
    let Ok(required) = required_retained_bytes(&queries, &response, API_VERSION) else {
        panic!("bounded diagnostic charge must be representable");
    };

    let Ok(normalized) =
        normalize_describe_configs_response(&queries, &response, API_VERSION, required)
    else {
        panic!("bounded diagnostic must normalize at its exact charge");
    };
    let Err(error) = &normalized.resources[0].outcome else {
        panic!("fixture must remain a broker failure");
    };
    let Some(message) = error.message.as_deref() else {
        panic!("bounded diagnostic must remain present");
    };
    assert_eq!(error.code.get(), -32_123);
    assert!(error.message_truncated);
    assert!(message.len() <= 1024);
    assert!(diagnostic.starts_with(message));
    assert!(message.is_char_boundary(message.len()));
}

fn successful_response(config: DescribeConfigsResourceResult) -> DescribeConfigsResponse {
    let mut result = DescribeConfigsResult::default();
    result.resource_type = 2;
    result.resource_name = "orders".into();
    result.error_code = 0;
    result.error_message = None;
    result.configs = vec![config];
    let mut response = DescribeConfigsResponse::default();
    response.results = vec![result];
    response
}
