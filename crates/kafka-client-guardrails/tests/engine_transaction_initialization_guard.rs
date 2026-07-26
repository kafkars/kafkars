//! Exact ownership and negative evidence for transaction initialization execution.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/transaction";
const HOST: &str = "TransactionInitializationHost";
const HOST_PATH: &str = "crates/kafka-client-engine/src/transaction/initialization/host.rs";
const RETAINED_OWNER: &str = "RetainedTransactionalOwner";
const RETAINED_OWNER_PATH: &str =
    "crates/kafka-client-engine/src/transaction/initialization/retained_owner.rs";
const RETAINED_OWNER_TEST: &str =
    "crates/kafka-client-engine/src/transaction/initialization/retained_owner_test.rs";
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::consumer",
    "crate::driver",
    "crate::producer",
    "kafka_driver",
    "kafka_wire",
    "tokio",
    "async_std",
    "smol",
    "Transport",
    "Retry",
];

#[test]
fn checked_in_transaction_execution_owners_and_capabilities_are_exact() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == HOST)
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, HOST_PATH);
    let retained = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == RETAINED_OWNER)
        .collect::<Vec<_>>();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].path, RETAINED_OWNER_PATH);
    assert!(
        config.test_mirrors.iter().any(|mirror| {
            mirror.production == RETAINED_OWNER_PATH && mirror.test == RETAINED_OWNER_TEST
        }),
        "retained owner needs one sibling test mirror"
    );

    for field in ["operations", "live_owners", "retained_bytes", "accepting"] {
        assert_eq!(
            config
                .mutation_owners
                .iter()
                .filter(|rule| rule.owner_type == HOST && rule.field == field)
                .count(),
            1,
            "{field} needs one mutation owner"
        );
    }
    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
    for forbidden in FORBIDDEN {
        assert!(
            capabilities[0]
                .forbidden
                .iter()
                .any(|value| value == forbidden)
        );
    }
    let submission = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "submit_tracked_transaction_init")
        .collect::<Vec<_>>();
    assert_eq!(submission.len(), 1);
    assert_eq!(
        submission[0].allowed_paths,
        ["crates/kafka-client-engine/src/driver/rpc/transaction_init_call.rs"]
    );
}

#[test]
fn fixture_rejects_clone_foreign_mutation_and_sibling_transport_theft() {
    let (root, files) = fixture_files("engine_transaction_initialization");
    let linear = linear_violations(
        &root,
        &files,
        &[LinearOwner {
            owner_type: HOST.into(),
            path: "src/linear_intruder.rs".into(),
        }],
    );
    assert!(
        linear
            .iter()
            .any(|violation| violation.contains("derives Clone"))
    );

    let mutations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: HOST.into(),
            field: "retained_bytes".into(),
            allowed_paths: vec!["src/mutation_owner.rs".into()],
        }],
    );
    assert!(
        mutations
            .iter()
            .any(|violation| violation.contains("mutation_intruder.rs"))
    );

    let capabilities = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in FORBIDDEN {
        assert!(
            capabilities
                .iter()
                .any(|violation| violation.contains(forbidden)),
            "fixture missed {forbidden}: {capabilities:?}"
        );
    }
}
