//! Ownership and capability ratchets for classic-group session identity.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, workspace_root,
};

const OWNER: &str = "GroupSessionCatalog";
const PATH: &str = "crates/kafka-client-engine/src/consumer/group/session_catalog.rs";
const PREPARED: &str = "crates/kafka-client-engine/src/consumer/group/session_catalog_prepared.rs";
const METHOD_ROOT: &str = "crates/kafka-client-engine/src";
const INSTALL_METHOD: &str = "install_group_session_replacement";
const LINEAR: &[(&str, &str)] = &[(OWNER, PATH), ("PreparedGroupSessionReplacement", PREPARED)];
const FIELDS: &[&str] = &[
    "next_member_id",
    "next_topic_id",
    "retained_topic_name_bytes",
    "topics_by_name",
    "topics_by_id",
    "current",
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::completion",
    "crate::driver",
    "crate::producer",
    "crate::protocol",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "Condvar",
    "Instant::now",
    "Mutex",
    "RwLock",
    "Future",
    "async",
    "Callback",
    "Metadata",
    "Transport",
    "Retry",
];

#[test]
fn checked_in_group_session_catalog_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR {
        let linear = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(linear.len(), 1);
        assert_eq!(linear[0].path, *path);
    }

    for field in FIELDS {
        let mutations = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == OWNER && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(mutations.len(), 1, "{field} needs one mutation rule");
        assert_eq!(mutations[0].allowed_paths, [PATH]);
    }

    for path in [PATH, PREPARED] {
        let capabilities = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == path)
            .collect::<Vec<_>>();
        assert_eq!(capabilities.len(), 1);
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

    let installers = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == INSTALL_METHOD)
        .collect::<Vec<_>>();
    assert_eq!(installers.len(), 1);
    assert_eq!(installers[0].root, METHOD_ROOT);
    assert_eq!(
        installers[0]
            .allowed_paths
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [PREPARED],
    );
}

#[test]
fn fixture_rejects_cloneable_catalog_and_foreign_field_mutation() {
    let (root, files) = fixture_files("consumer_group_session_catalog");
    let linear_rules = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let linear = linear_violations(&root, &files, &linear_rules);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(linear.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutation_rules = FIELDS
        .iter()
        .map(|field| MutationOwner {
            owner_type: OWNER.into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mutations = mutation_violations(&root, &files, &mutation_rules);
    for field in FIELDS {
        assert!(mutations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(OWNER)
                && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_protocol_execution_runtime_and_sibling_policy() {
    let (root, _files) = fixture_files("consumer_group_session_catalog");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}

#[test]
fn fixture_rejects_sibling_catalog_installation() {
    let (root, _files) = fixture_files("consumer_group_session_catalog");
    let violations = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: INSTALL_METHOD.into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("method_intruder.rs") && violation.contains(INSTALL_METHOD)
    }));
}
