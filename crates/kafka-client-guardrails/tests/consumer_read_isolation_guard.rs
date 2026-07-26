//! Ownership and protocol-decoder ratchets for read-committed visibility.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, MutationOwner, call_capability_violations,
    capability_violations, fixture_files, load_config, mutation_violations, rust_files,
    workspace_root,
};

const FILTER: &str = "crates/kafka-client-engine/src/protocol/fetch/read_committed.rs";
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::consumer",
    "crate::driver",
    "crate::producer",
    "crate::transaction",
    "kafka_client_core",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "ControlRecordTypeSchema",
    "Decoder",
    "EndTxnMarker",
    "KafkaDecode",
    "std::future",
    "std::time",
    "Transport",
    "Retry",
    "async",
];

#[test]
fn checked_in_read_isolation_policy_is_exact() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    for (production, test) in [
        (
            "crates/kafka-client-core/src/consumer/read_isolation.rs",
            "crates/kafka-client-core/src/consumer/read_isolation_test.rs",
        ),
        (
            "crates/kafka-client-engine/src/protocol/fetch/control_record.rs",
            "crates/kafka-client-engine/src/protocol/fetch/control_record_test.rs",
        ),
        (
            "crates/kafka-client-engine/src/protocol/fetch/isolation.rs",
            "crates/kafka-client-engine/src/protocol/fetch/isolation_test.rs",
        ),
        (
            FILTER,
            "crates/kafka-client-engine/src/protocol/fetch/read_committed_test.rs",
        ),
    ] {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }

    let mutation = config
        .mutation_owners
        .iter()
        .filter(|rule| {
            rule.owner_type == "AssignedConsumerMachine" && rule.field == "read_isolation"
        })
        .collect::<Vec<_>>();
    assert!(
        mutation.is_empty(),
        "immutable read isolation must not carry a decorative mutation permission"
    );
    let immutable = mutation_violations(
        &workspace,
        &rust_files(&workspace, &config),
        &[MutationOwner {
            owner_type: "AssignedConsumerMachine".into(),
            field: "read_isolation".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert_eq!(
        immutable,
        vec![
            "decorative mutation rule: AssignedConsumerMachine.read_isolation has no detected mutations"
                .to_owned()
        ],
        "immutable read isolation gained a post-construction mutation"
    );

    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == FILTER)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
    assert_eq!(
        capabilities[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    assert!(capabilities[0].allow.is_empty());

    let calls = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "decode_control_record")
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].allowed_paths, vec![FILTER.to_owned()]);
}

#[test]
fn fixture_rejects_policy_mutation_raw_decoding_and_foreign_calls() {
    let (root, files) = fixture_files("consumer_read_isolation");
    let mutations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "AssignedConsumerMachine".into(),
            field: "read_isolation".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(mutations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs") && violation.contains("read_isolation")
    }));

    let capabilities = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in [
        "kafka_driver",
        "kafka_wire_core",
        "ControlRecordTypeSchema",
        "Decoder",
        "std::time",
        "Retry",
        "async",
    ] {
        assert!(
            capabilities
                .iter()
                .any(|violation| violation.contains(capability)),
            "capability detector missed {capability}: {capabilities:?}"
        );
    }

    let calls = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".into(),
            call: "decode_control_record".into(),
            allowed_paths: vec!["src/read_committed.rs".into()],
        }],
    );
    assert!(calls.iter().any(|violation| {
        violation.contains("capability_intruder.rs") && violation.contains("decode_control_record")
    }));
}
