//! Negative evidence for assigned group-offset protocol confinement.

mod support;

use support::{
    CapabilityRule, LinearOwner, capability_violations, fixture_files, linear_violations,
    load_config, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/protocol/consumer/group_offset_fetch";
const MODEL: &str = "crates/kafka-client-engine/src/protocol/consumer/group_offset_fetch/model.rs";
const PREPARATION: &str =
    "crates/kafka-client-engine/src/protocol/consumer/group_offset_fetch/preparation.rs";
const REQUEST: &str =
    "crates/kafka-client-engine/src/protocol/consumer/group_offset_fetch/request.rs";
const LINEAR: &[(&str, &str)] = &[
    ("GroupOffsetFetchCorrelation", MODEL),
    ("PreparedGroupOffsetFetch", PREPARATION),
    ("PreparedGroupOffsetFetchRequest", PREPARATION),
    ("GroupOffsetFetchRequest", REQUEST),
];
const RAW_DTOS: &[&str] = &[
    "OffsetFetchRequest",
    "OffsetFetchRequestGroup",
    "OffsetFetchRequestTopic",
    "OffsetFetchRequestTopics",
    "OffsetFetchResponse",
    "OffsetFetchResponseGroup",
    "OffsetFetchResponsePartition",
    "OffsetFetchResponsePartitions",
    "OffsetFetchResponseTopic",
    "OffsetFetchResponseTopics",
];

#[test]
fn checked_in_protocol_confinement_and_linear_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    let protocol = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(protocol.len(), 1);
    assert!(
        protocol[0]
            .forbidden
            .iter()
            .any(|value| value == "crate::admin")
    );
    assert!(
        protocol[0]
            .forbidden
            .iter()
            .any(|value| value == "kafka_client_core")
    );
    let confinement = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == "crates/kafka-client-engine/src/consumer")
        .find(|rule| rule.forbidden.iter().any(|value| value == RAW_DTOS[0]))
        .unwrap_or_else(|| panic!("consumer integration needs OffsetFetch DTO confinement"));
    assert_eq!(
        confinement
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        RAW_DTOS
    );
}

#[test]
fn fixture_rejects_cloneable_owners_and_raw_generated_dtos() {
    let (root, files) = fixture_files("consumer_group_offset_fetch_protocol");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".to_owned(),
            forbidden: RAW_DTOS.iter().map(|value| (*value).to_owned()).collect(),
            allow: Vec::new(),
        }],
    );
    for dto in RAW_DTOS {
        assert!(violations.iter().any(|violation| {
            violation.contains("generated_dto_intruder.rs") && violation.contains(dto)
        }));
    }
}
