//! Ownership and protocol-decoder ratchets for read-committed visibility.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, MutationOwner, call_capability_violations,
    capability_violations, fixture_files, load_config, mutation_violations, read, rust_files,
    workspace_root,
};

const FILTER: &str = "crates/kafka-client-engine/src/protocol/fetch/read_committed.rs";
const GROUP_REGISTRATION: &str =
    "crates/kafka-client-engine/src/consumer/group_registration_request.rs";
const GROUP_ENTRY: &str = "crates/kafka-client-engine/src/consumer/group/registry_entry.rs";
const GROUP_FETCH_BUILD: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner_build.rs";
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
#[expect(
    clippy::too_many_lines,
    reason = "one policy test checks the complete read-isolation ownership ratchet"
)]
fn checked_in_read_isolation_policy_is_exact() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
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

    let immutable_machine = config
        .mutation_owners
        .iter()
        .filter(|rule| {
            rule.owner_type == "AssignedConsumerMachine" && rule.field == "read_isolation"
        })
        .collect::<Vec<_>>();
    assert!(
        immutable_machine.is_empty(),
        "immutable read isolation must not carry a decorative mutation permission"
    );
    let immutable = mutation_violations(
        &workspace,
        &files,
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

    let registration = config
        .mutation_owners
        .iter()
        .filter(|rule| {
            rule.owner_type == "GroupConsumerRegistration" && rule.field == "read_isolation"
        })
        .collect::<Vec<_>>();
    assert_eq!(registration.len(), 1);
    assert_eq!(registration[0].allowed_paths, [GROUP_REGISTRATION]);
    assert!(
        mutation_violations(
            &workspace,
            &files,
            &[MutationOwner {
                owner_type: "GroupConsumerRegistration".into(),
                field: "read_isolation".into(),
                allowed_paths: vec![GROUP_REGISTRATION.into()],
            }],
        )
        .is_empty(),
        "registration read isolation escaped its sole inert configuration owner"
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
fn hosted_registration_isolation_is_non_decorative_and_immutable() {
    let root = workspace_root();
    let registration = read(&root.join(GROUP_REGISTRATION));
    for token in [
        "read_isolation: ConsumerReadIsolation",
        "read_isolation: ConsumerReadIsolation::ReadUncommitted",
        "self.read_isolation = read_isolation",
        "self.read_isolation,",
    ] {
        assert!(registration.contains(token), "registration lost {token}");
    }

    let entry = read(&root.join(GROUP_ENTRY));
    assert!(entry.contains("pub(super) read_isolation: ReadIsolation"));
    let fetch_construction = concat!(
        "ClassicGroupFetchOwner::try_new_with_fetch_configuration(\n",
        "                read_isolation,\n",
        "                missing_offset_policy,\n",
        "                fetch,\n",
        "                limits,\n",
        "            )",
    );
    assert_eq!(entry.matches(fetch_construction).count(), 1);

    let owner = read(&root.join(GROUP_FETCH_BUILD));
    for token in [
        "AssignedConsumerMachine::with_read_isolation(read_isolation)",
        ".with_isolation(fetch_isolation(read_isolation))",
    ] {
        assert!(owner.contains(token), "hosted Fetch owner lost {token}");
    }
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
