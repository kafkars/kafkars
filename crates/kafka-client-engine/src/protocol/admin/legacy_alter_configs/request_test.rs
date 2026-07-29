//! Generated request semantics across the complete stable API-key 33 range.

use kafka_client_core::{
    LegacyAlterConfigsPlan, LegacyConfigEntry, LegacyConfigResourceReplacement,
    LegacyTopicConfigReplacement,
};
use kafka_wire::AlterConfigsRequest;
use kafka_wire_core::{
    ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes,
};

use super::request::legacy_alter_configs_request;

#[test]
fn request_preserves_full_snapshot_order_nullable_values_and_validate_only() {
    let request = legacy_alter_configs_request(&plan());

    assert!(request.validate_only);
    assert_eq!(request.resources.len(), 2);
    assert_eq!(request.resources[0].resource_type, 2);
    assert_eq!(request.resources[0].resource_name.as_str(), "orders");
    assert_eq!(request.resources[1].resource_name.as_str(), "audit");
    assert!(request.resources[1].configs.is_empty());
    assert_eq!(
        request.resources[0]
            .configs
            .iter()
            .map(|config| config.name.as_str())
            .collect::<Vec<_>>(),
        vec!["cleanup.policy", "retention.ms", "segment.bytes"]
    );
    assert_eq!(
        request.resources[0]
            .configs
            .iter()
            .map(|config| config.value.as_ref().map(StrBytes::as_str))
            .collect::<Vec<_>>(),
        vec![Some("compact"), None, Some("")]
    );
}

#[test]
fn nullable_values_round_trip_in_v0_v1_and_flexible_v2() {
    let request = legacy_alter_configs_request(&plan());
    for version in [0, 1, 2] {
        assert_round_trip(&request, ApiVersion::new(version));
    }
}

#[test]
fn generic_request_preserves_exact_known_future_resource_types_and_empty_snapshots() {
    let plan = generic_plan();
    let request = legacy_alter_configs_request(&plan);

    assert!(request.validate_only);
    assert_eq!(
        request
            .resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str()))
            .collect::<Vec<_>>(),
        [
            (4, "1"),
            (8, "1"),
            (16, "payments-client"),
            (32, "payments-group"),
            (64, "future-resource"),
        ]
    );
    assert!(request.resources[1].configs.is_empty());
    assert_eq!(
        request.resources[2].configs[0]
            .value
            .as_ref()
            .map(StrBytes::as_str),
        Some("")
    );
    assert!(request.resources[3].configs[0].value.is_none());
    for version in [0, 1, 2] {
        assert_round_trip(&request, ApiVersion::new(version));
    }
}

fn assert_round_trip(request: &AlterConfigsRequest, version: ApiVersion) {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("generated v{version} request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("generated request is bounded: {error}"));
    let decoded = AlterConfigsRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("generated v{version} request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("generated request consumes its frame: {error}"));
    assert_eq!(
        decoded.resources[0]
            .configs
            .iter()
            .map(|config| config.value.as_ref().map(StrBytes::as_str))
            .collect::<Vec<_>>(),
        request.resources[0]
            .configs
            .iter()
            .map(|config| config.value.as_ref().map(StrBytes::as_str))
            .collect::<Vec<_>>()
    );
}

fn plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::new(
        vec![
            LegacyTopicConfigReplacement::new(
                "orders".to_owned(),
                vec![
                    LegacyConfigEntry::new("cleanup.policy".to_owned(), Some("compact".to_owned())),
                    LegacyConfigEntry::new("retention.ms".to_owned(), None),
                    LegacyConfigEntry::new("segment.bytes".to_owned(), Some(String::new())),
                ],
            ),
            LegacyTopicConfigReplacement::new("audit".to_owned(), Vec::new()),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid legacy replacement plan: {error}"))
}

fn generic_plan() -> LegacyAlterConfigsPlan {
    LegacyAlterConfigsPlan::for_resources(
        vec![
            resource(4, "1", vec![entry("broker.key", Some("broker-value"))]),
            resource(8, "1", Vec::new()),
            resource(16, "payments-client", vec![entry("metrics", Some(""))]),
            resource(
                32,
                "payments-group",
                vec![entry("consumer.session.timeout.ms", None)],
            ),
            resource(
                64,
                "future-resource",
                vec![entry("future.key", Some("future"))],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid generic legacy plan: {error}"))
}

fn resource(
    resource_type: i8,
    resource_name: &str,
    configs: Vec<LegacyConfigEntry>,
) -> LegacyConfigResourceReplacement {
    LegacyConfigResourceReplacement::resource(resource_type, resource_name.to_owned(), configs)
}

fn entry(key: &str, value: Option<&str>) -> LegacyConfigEntry {
    LegacyConfigEntry::new(key.to_owned(), value.map(str::to_owned))
}
