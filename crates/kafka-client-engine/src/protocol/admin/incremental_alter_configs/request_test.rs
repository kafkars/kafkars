//! Generated request semantics for all incremental configuration operations.

use kafka_client_core::{
    ConfigAlteration, IncrementalAlterConfigsPlan, IncrementalConfigResourceAlteration,
    TopicConfigAlteration,
};
use kafka_wire::IncrementalAlterConfigsRequest;
use kafka_wire_core::{
    ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes,
};

use super::request::incremental_alter_configs_request;

#[test]
fn request_uses_generated_v0_v1_topic_operations_without_legacy_fallback() {
    let plan = IncrementalAlterConfigsPlan::new(
        vec![
            TopicConfigAlteration::new(
                "orders".to_owned(),
                vec![
                    ConfigAlteration::set("cleanup.policy".to_owned(), "compact".to_owned()),
                    ConfigAlteration::delete("retention.ms".to_owned()),
                    ConfigAlteration::append("remote.log.tags.add".to_owned(), String::new()),
                    ConfigAlteration::subtract(
                        "remote.log.tags.remove".to_owned(),
                        "cold".to_owned(),
                    ),
                ],
            ),
            TopicConfigAlteration::new(
                "audit".to_owned(),
                vec![ConfigAlteration::set(
                    "cleanup.policy".to_owned(),
                    "delete".to_owned(),
                )],
            ),
        ],
        true,
    )
    .unwrap_or_else(|error| panic!("valid incremental plan: {error}"));

    let request = incremental_alter_configs_request(&plan);

    assert!(request.validate_only);
    assert_eq!(request.resources.len(), 2);
    assert_eq!(request.resources[0].resource_type, 2);
    assert_eq!(request.resources[0].resource_name.as_str(), "orders");
    assert_eq!(request.resources[1].resource_name.as_str(), "audit");
    assert_eq!(
        request.resources[0]
            .configs
            .iter()
            .map(|config| config.config_operation)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        request.resources[0]
            .configs
            .iter()
            .map(|config| config.value.as_ref().map(StrBytes::as_str))
            .collect::<Vec<_>>(),
        vec![Some("compact"), None, Some(""), Some("cold")]
    );
    assert_nullable_values_round_trip(&request, ApiVersion::new(0));
    assert_nullable_values_round_trip(&request, ApiVersion::new(1));
}

#[test]
fn request_preserves_known_and_future_resource_types_in_caller_order() {
    let plan = IncrementalAlterConfigsPlan::for_resources(
        vec![
            resource(4, "1"),
            resource(8, "1"),
            resource(16, "client"),
            resource(32, "group"),
            resource(64, "future"),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid generic plan: {error}"));

    let request = incremental_alter_configs_request(&plan);
    assert!(!request.validate_only);
    assert_eq!(
        request
            .resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str()))
            .collect::<Vec<_>>(),
        [
            (4, "1"),
            (8, "1"),
            (16, "client"),
            (32, "group"),
            (64, "future"),
        ]
    );
    assert_nullable_values_round_trip(&request, ApiVersion::new(0));
    assert_nullable_values_round_trip(&request, ApiVersion::new(1));
}

fn resource(resource_type: i8, name: &str) -> IncrementalConfigResourceAlteration {
    IncrementalConfigResourceAlteration::resource(
        resource_type,
        name.to_owned(),
        vec![ConfigAlteration::delete("key".to_owned())],
    )
}

fn assert_nullable_values_round_trip(
    request: &IncrementalAlterConfigsRequest,
    version: ApiVersion,
) {
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("generated v{version} request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("generated request frame is bounded: {error}"));
    let decoded = IncrementalAlterConfigsRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("generated v{version} request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("generated request consumes its frame: {error}"));
    assert_eq!(&decoded, request);
}
