//! Exact ownership registration and negative evidence for transaction initialization.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const ROOT: &str = "crates/kafka-client-core/src/transaction";
const MACHINE: &str = "TransactionInitializationMachine";
const MACHINE_PATH: &str = "crates/kafka-client-core/src/transaction/initialization/machine.rs";
const TRANSITION_PATH: &str =
    "crates/kafka-client-core/src/transaction/initialization/transition.rs";
const MIRRORS: &[(&str, &str)] = &[
    (
        MACHINE_PATH,
        "crates/kafka-client-core/src/transaction/initialization/machine_test.rs",
    ),
    (
        "crates/kafka-client-core/src/transaction/initialization/model.rs",
        "crates/kafka-client-core/src/transaction/initialization/model_test.rs",
    ),
    (
        "crates/kafka-client-core/src/transaction/initialization/outcome.rs",
        "crates/kafka-client-core/src/transaction/initialization/outcome_test.rs",
    ),
    (
        TRANSITION_PATH,
        "crates/kafka-client-core/src/transaction/initialization/transition_test.rs",
    ),
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::consumer",
    "crate::producer",
    "Callback",
    "Clock",
    "Coordinator",
    "Engine",
    "Future",
    "Generated",
    "Metadata",
    "Retry",
    "Runtime",
    "String",
    "Transport",
    "Wire",
    "async",
    "async_std",
    "bytes",
    "kafka_client_engine",
    "kafka_driver",
    "kafka_wire",
    "smol",
    "std::future",
    "std::time",
    "tokio",
];

#[test]
fn transaction_initialization_owner_is_registered_at_exact_core_modules() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == MACHINE)
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, MACHINE_PATH);

    let mutation = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == MACHINE && rule.field == "state")
        .collect::<Vec<_>>();
    assert_eq!(mutation.len(), 1);
    assert_eq!(mutation[0].allowed_paths, [MACHINE_PATH, TRANSITION_PATH]);

    let capability = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(capability.len(), 1);
    assert_eq!(
        capability[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );

    for (production, test) in MIRRORS {
        let mirror = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(mirror.len(), 1, "{production} needs one test mirror");
        assert_eq!(mirror[0].test, *test);
    }
}

#[test]
fn fixture_rejects_foreign_transaction_initialization_state_mutation() {
    let (root, files) = fixture_files("transaction_initialization_ownership");
    let violations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: MACHINE.into(),
            field: "state".into(),
            allowed_paths: vec!["src/mutation_owner.rs".into()],
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains(MACHINE)
            && violation.contains("state")
    }));
}

#[test]
fn fixture_rejects_cloneable_transaction_initialization_owner() {
    let (root, files) = fixture_files("transaction_initialization_ownership");
    let violations = linear_violations(
        &root,
        &files,
        &[LinearOwner {
            owner_type: MACHINE.into(),
            path: "src/linear_intruder.rs".into(),
        }],
    );
    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(MACHINE) && violation.contains(derived))
        );
    }
}

#[test]
fn fixture_rejects_foreign_transaction_capabilities() {
    let (root, _files) = fixture_files("transaction_initialization_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(capability)),
            "capability fixture missed {capability}: {violations:?}"
        );
    }
}
