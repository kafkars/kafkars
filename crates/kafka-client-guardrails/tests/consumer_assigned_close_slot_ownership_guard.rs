//! Ownership, capability, and construction ratchets for assigned close retention.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, LinearOwner, MutationOwner, call_capability_violations,
    capability_violations, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const ERROR: &str = "crates/kafka-client-engine/src/consumer/assigned_close_error.rs";
const SLOT: &str = "crates/kafka-client-engine/src/consumer/assigned_close_slot.rs";
const SLOT_TEST: &str = "crates/kafka-client-engine/src/consumer/assigned_close_slot_test.rs";
const PUBLICATION: &str =
    "crates/kafka-client-engine/src/consumer/assigned_close_slot/publication.rs";
const PUBLICATION_TEST: &str =
    "crates/kafka-client-engine/src/consumer/assigned_close_slot/publication_test.rs";
const LINEAR: &[&str] = &["AssignedCloseState", "AssignedCloseSlot"];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::collections",
    "std::future",
    "std::net",
    "std::sync",
    "std::thread",
    "std::time",
    "Arc",
    "Box",
    "Future",
    "String",
    "Vec",
    "async",
];

#[test]
fn checked_in_close_slot_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (production, test) in [(SLOT, SLOT_TEST), (PUBLICATION, PUBLICATION_TEST)] {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, test);
    }
    for owner_type in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, SLOT);
    }

    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedCloseSlot" && rule.field == "state")
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].allowed_paths, [SLOT, PUBLICATION]);

    for root in [ERROR, SLOT, PUBLICATION] {
        let capabilities = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == root)
            .collect::<Vec<_>>();
        assert_eq!(capabilities.len(), 1, "{root} needs one capability rule");
        assert_eq!(
            capabilities[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN,
        );
        assert!(capabilities[0].allow.is_empty());
    }

    let constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "AssignedCloseSlot::create_for_assigned_owner")
        .collect::<Vec<_>>();
    assert_eq!(constructors.len(), 1);
    assert_eq!(
        constructors[0].allowed_paths,
        ["crates/kafka-client-engine/src/consumer/assigned_owner.rs"]
    );
}

#[test]
fn fixture_rejects_duplication_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_assigned_close_slot_ownership");
    let linear = LINEAR
        .iter()
        .map(|owner_type| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for owner_type in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let violations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "AssignedCloseSlot".into(),
            field: "state".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("AssignedCloseSlot")
            && violation.contains("state")
    }));
}

#[test]
fn fixture_rejects_allocation_runtime_and_sibling_capabilities() {
    let (root, _) = fixture_files("consumer_assigned_close_slot_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in [
        "crate::driver",
        "crate::producer",
        "kafka_driver",
        "kafka_wire",
        "std::collections",
        "std::future",
        "std::net",
        "std::sync",
        "std::thread",
        "std::time",
        "Arc",
        "Box",
        "Future",
        "String",
        "Vec",
        "async",
    ] {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}

#[test]
fn fixture_rejects_constructor_use_outside_the_owner() {
    let root = fixture_files("consumer_assigned_close_slot_ownership").0;
    let violations = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".into(),
            call: "AssignedCloseSlot::create_for_assigned_owner".into(),
            allowed_paths: vec!["src/assigned_owner.rs".into()],
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("constructor_intruder.rs")
            && violation.contains("AssignedCloseSlot::create_for_assigned_owner")
    }));
}
