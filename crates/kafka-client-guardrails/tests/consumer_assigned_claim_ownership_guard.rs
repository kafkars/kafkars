//! Exact ownership ratchets for assigned-consumer claim and handle transfer.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, LinearOwner, MutationOwner, call_capability_violations,
    capability_violations, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const CLAIM: &str = "crates/kafka-client-engine/src/consumer/assigned_host/claim.rs";
const CLAIM_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_host/claim_test.rs";
const HANDLE: &str = "crates/kafka-client-engine/src/consumer/assigned_host/handle.rs";
const HANDLE_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_host/handle_test.rs";
const LINEAR: &[(&str, &str)] = &[
    ("AssignedConsumerClaimSlot", CLAIM),
    ("AssignedConsumerAdmissionCloser", CLAIM),
    ("AssignedConsumerHandle", HANDLE),
];
const FORBIDDEN: &[&str] = &[
    "kafka_driver",
    "kafka_wire",
    "tokio",
    "async_std",
    "smol",
    "std::thread",
    "std::time::Instant",
    "std::time::SystemTime",
    "Callback",
    "Metadata",
    "Retry",
    "async",
];

#[test]
fn checked_in_claim_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in [(CLAIM, CLAIM_TEST), (HANDLE, HANDLE_TEST)] {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    for path in [CLAIM, HANDLE] {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN,
        );
        assert!(rules[0].allow.is_empty());
    }

    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerClaimSlot" && rule.field == "port")
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].allowed_paths, [CLAIM]);

    let constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "AssignedConsumerClaimSlot::create_for_engine")
        .collect::<Vec<_>>();
    assert_eq!(constructors.len(), 1);
    assert_eq!(constructors[0].root, "crates/kafka-client-engine/src");
    assert_eq!(
        constructors[0].allowed_paths,
        ["crates/kafka-client-engine/src/engine.rs"]
    );
}

#[test]
fn fixture_rejects_duplication_mutation_runtime_and_constructor_theft() {
    let (root, files) = fixture_files("consumer_assigned_claim_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/violations.rs".into(),
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

    let mutations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "AssignedConsumerClaimSlot".into(),
            field: "port".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(mutations.iter().any(|violation| {
        violation.contains("violations.rs")
            && violation.contains("AssignedConsumerClaimSlot")
            && violation.contains("port")
    }));

    let capabilities = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in FORBIDDEN {
        assert!(
            capabilities
                .iter()
                .any(|violation| violation.contains(forbidden)),
            "missed {forbidden}: {capabilities:?}"
        );
    }

    let constructors = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".into(),
            call: "AssignedConsumerClaimSlot::create_for_engine".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(constructors.iter().any(|violation| {
        violation.contains("violations.rs")
            && violation.contains("AssignedConsumerClaimSlot::create_for_engine")
    }));
}
