//! Ownership and capability ratchets for assignment topic identities.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const OWNER: &str = "AssignedTopics";
const PATH: &str = "crates/kafka-client-engine/src/consumer/assigned_topics.rs";
const PREPARED: &str = "crates/kafka-client-engine/src/consumer/assigned_topics/prepared.rs";
const INCREMENTAL: &str = "crates/kafka-client-engine/src/consumer/assigned_topics/incremental.rs";
const LINEAR: &[(&str, &str)] = &[
    (OWNER, PATH),
    ("PreparedAssignedTopicsReplacement", PREPARED),
    ("PreparedAssignedTopicsAddition", INCREMENTAL),
    ("PreparedAssignedTopicsRemoval", INCREMENTAL),
];
const FIELDS: &[&str] = &[
    "next_topic_id",
    "retained_name_bytes",
    "by_name",
    "by_id",
    "partitions",
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
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
    "Metadata",
    "Transport",
    "Retry",
];

#[test]
fn checked_in_assigned_topics_policy_is_exact() {
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

    for path in [PATH, PREPARED, INCREMENTAL] {
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
}

#[test]
fn fixture_rejects_cloneable_owner_and_foreign_field_mutation() {
    let (root, files) = fixture_files("consumer_assigned_topics_ownership");
    let rules = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let linear = linear_violations(&root, &files, &rules);
    for (owner_type, _) in LINEAR {
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
fn fixture_rejects_foreign_protocol_transport_and_policy_capabilities() {
    let (root, _) = fixture_files("consumer_assigned_topics_ownership");
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
