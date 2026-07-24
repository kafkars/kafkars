//! Scenarios for ordered generated `DescribeConfigs` response normalization.

use kafka_wire::{
    DescribeConfigsResponse,
    describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    },
};

use super::describe_configs::DescribeConfigsQuery;
use super::describe_configs_response::{
    DescribeConfigsProtocolFailure, normalize_describe_configs_response,
};

const LARGE_BUDGET: usize = 1 << 20;
const API_VERSION: i16 = 4;

#[test]
fn shuffled_response_becomes_ordered_wire_free_scalar_facts() {
    let keys = ["cleanup.policy", "retention.ms"];
    let queries = [query(2, "orders", Some(&keys)), query(4, "7", None)];
    let mut cleanup = config("cleanup.policy", Some("compact"));
    cleanup.read_only = true;
    cleanup.config_source = 5;
    cleanup.config_type = 2;
    cleanup.documentation = Some("cleanup docs".into());
    cleanup.synonyms = vec![synonym("zeta", Some("z"), 2), synonym("alpha", None, -7)];
    let retention = config("retention.ms", Some("60000"));
    let topic = success(2, "orders", vec![retention, cleanup]);
    let broker = failure(4, "7", -32_123, "future error");
    let response = response(77, vec![broker, topic]);

    let Ok(normalized) =
        normalize_describe_configs_response(&queries, &response, API_VERSION, LARGE_BUDGET)
    else {
        panic!("structurally valid response must normalize");
    };

    assert_eq!(normalized.throttle_time_ms, 77);
    assert_eq!(normalized.resources.len(), 2);
    let topic = &normalized.resources[0];
    assert_eq!(
        (topic.resource_type, topic.resource_name.as_str()),
        (2, "orders")
    );
    let Ok(configs) = &topic.outcome else {
        panic!("topic fixture must succeed");
    };
    assert_eq!(
        configs
            .iter()
            .map(|config| config.name.as_str())
            .collect::<Vec<_>>(),
        keys
    );
    assert_eq!(configs[0].value.as_deref(), Some("compact"));
    assert!(configs[0].read_only);
    assert_eq!(configs[0].source, 5);
    assert!(!configs[0].sensitive);
    assert_eq!(configs[0].config_type, Some(2));
    assert_eq!(configs[0].documentation.as_deref(), Some("cleanup docs"));
    assert_eq!(configs[0].synonyms[0].name, "alpha");
    assert_eq!(configs[0].synonyms[0].source, -7);
    assert_eq!(configs[0].synonyms[0].value, None);
    let broker = &normalized.resources[1];
    let Err(error) = &broker.outcome else {
        panic!("broker fixture must fail");
    };
    assert_eq!(error.code.get(), -32_123);
    assert_eq!(error.message.as_deref(), Some("future error"));
    assert!(!error.message_truncated);
}

#[test]
fn all_configuration_query_sorts_entries_deterministically() {
    let queries = [query(2, "orders", None)];
    let response = response(
        0,
        vec![success(
            2,
            "orders",
            vec![config("zeta", None), config("alpha", None)],
        )],
    );

    let Ok(normalized) =
        normalize_describe_configs_response(&queries, &response, API_VERSION, LARGE_BUDGET)
    else {
        panic!("all-config response must normalize");
    };
    let Ok(configs) = &normalized.resources[0].outcome else {
        panic!("resource must succeed");
    };
    assert_eq!(
        configs
            .iter()
            .map(|config| config.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
}

#[test]
fn ambiguous_generated_shapes_never_bind_to_the_wrong_request() {
    let keys = ["cleanup.policy"];
    let queries = [query(2, "orders", Some(&keys))];
    let duplicated_resources = response(
        0,
        vec![
            success(2, "orders", Vec::new()),
            success(2, "orders", Vec::new()),
        ],
    );
    assert_eq!(
        normalize_describe_configs_response(
            &queries,
            &duplicated_resources,
            API_VERSION,
            LARGE_BUDGET,
        ),
        Err(DescribeConfigsProtocolFailure::ResourceCount)
    );

    let duplicated_configs = response(
        0,
        vec![success(
            2,
            "orders",
            vec![
                config("cleanup.policy", None),
                config("cleanup.policy", None),
            ],
        )],
    );
    assert_eq!(
        normalize_describe_configs_response(
            &queries,
            &duplicated_configs,
            API_VERSION,
            LARGE_BUDGET,
        ),
        Err(DescribeConfigsProtocolFailure::DuplicateConfig)
    );

    let unexpected = response(
        0,
        vec![success(2, "payments", vec![config("cleanup.policy", None)])],
    );
    assert_eq!(
        normalize_describe_configs_response(&queries, &unexpected, API_VERSION, LARGE_BUDGET,),
        Err(DescribeConfigsProtocolFailure::UnexpectedResource)
    );
}

#[test]
fn unrecognized_selected_key_may_be_absent_without_shape_failure() {
    let keys = ["cleanup.policy", "future.config"];
    let queries = [query(2, "orders", Some(&keys))];
    let response = response(
        0,
        vec![success(
            2,
            "orders",
            vec![config("cleanup.policy", Some("compact"))],
        )],
    );

    let Ok(normalized) =
        normalize_describe_configs_response(&queries, &response, API_VERSION, LARGE_BUDGET)
    else {
        panic!("a broker may omit an unknown selected key");
    };
    let Ok(configs) = &normalized.resources[0].outcome else {
        panic!("resource must succeed");
    };
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].name, "cleanup.policy");
}

#[test]
fn negative_throttle_and_ambiguous_queries_are_rejected() {
    let negative_response = response(-1, Vec::new());
    assert_eq!(
        normalize_describe_configs_response(&[], &negative_response, API_VERSION, LARGE_BUDGET,),
        Err(DescribeConfigsProtocolFailure::ThrottleTime)
    );

    let duplicated = [query(2, "orders", None), query(2, "orders", None)];
    let response = response(
        0,
        vec![
            success(2, "orders", Vec::new()),
            success(2, "orders", Vec::new()),
        ],
    );
    assert_eq!(
        normalize_describe_configs_response(&duplicated, &response, API_VERSION, LARGE_BUDGET,),
        Err(DescribeConfigsProtocolFailure::DuplicateRequestedResource)
    );
}

#[test]
fn pre_v3_generated_defaults_do_not_fabricate_type_or_documentation() {
    let queries = [query(2, "orders", None)];
    let mut entry = config("cleanup.policy", Some("compact"));
    entry.config_type = 0;
    entry.documentation = Some("".into());
    let response = response(0, vec![success(2, "orders", vec![entry])]);

    let Ok(normalized) = normalize_describe_configs_response(&queries, &response, 2, LARGE_BUDGET)
    else {
        panic!("v2 response must normalize");
    };
    let Ok(configs) = &normalized.resources[0].outcome else {
        panic!("resource must succeed");
    };
    assert_eq!(configs[0].config_type, None);
    assert_eq!(configs[0].documentation, None);

    assert_eq!(
        normalize_describe_configs_response(&queries, &response, 0, LARGE_BUDGET),
        Err(DescribeConfigsProtocolFailure::ApiVersion)
    );
}

fn query<'a>(
    resource_type: i8,
    resource_name: &'a str,
    configuration_keys: Option<&'a [&'a str]>,
) -> DescribeConfigsQuery<'a> {
    DescribeConfigsQuery {
        resource_type,
        resource_name,
        configuration_keys,
    }
}

fn response(throttle_time_ms: i32, results: Vec<DescribeConfigsResult>) -> DescribeConfigsResponse {
    let mut response = DescribeConfigsResponse::default();
    response.throttle_time_ms = throttle_time_ms;
    response.results = results;
    response
}

fn success(
    resource_type: i8,
    resource_name: &str,
    configs: Vec<DescribeConfigsResourceResult>,
) -> DescribeConfigsResult {
    let mut result = DescribeConfigsResult::default();
    result.error_code = 0;
    result.error_message = None;
    result.resource_type = resource_type;
    result.resource_name = resource_name.into();
    result.configs = configs;
    result
}

fn failure(
    resource_type: i8,
    resource_name: &str,
    code: i16,
    message: &str,
) -> DescribeConfigsResult {
    let mut result = success(resource_type, resource_name, Vec::new());
    result.error_code = code;
    result.error_message = Some(message.into());
    result
}

fn config(name: &str, value: Option<&str>) -> DescribeConfigsResourceResult {
    let mut config = DescribeConfigsResourceResult::default();
    config.name = name.into();
    config.value = value.map(Into::into);
    config.documentation = None;
    config
}

fn synonym(name: &str, value: Option<&str>, source: i8) -> DescribeConfigsSynonym {
    let mut synonym = DescribeConfigsSynonym::default();
    synonym.name = name.into();
    synonym.value = value.map(Into::into);
    synonym.source = source;
    synonym
}
