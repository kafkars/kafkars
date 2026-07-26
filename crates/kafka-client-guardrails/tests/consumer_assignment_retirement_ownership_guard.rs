//! Ownership and capability ratchets for exact assignment retirement.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, read, workspace_root,
};

const SHARED_ERROR: &str = "crates/kafka-client-core/src/consumer/error.rs";
const MODEL: &str = "crates/kafka-client-core/src/consumer/assignment_retirement.rs";
const TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/assignment_retirement_transition.rs";
const MIRRORS: &[(&str, &str)] = &[
    (
        MODEL,
        "crates/kafka-client-core/src/consumer/assignment_retirement_test.rs",
    ),
    (
        TRANSITION,
        "crates/kafka-client-core/src/consumer/assignment_retirement_transition_test.rs",
    ),
];
const LINEAR: &[(&str, &str)] = &[
    ("RetireAssignment", MODEL),
    ("RetireAssignmentError", MODEL),
];
const FORBIDDEN: &[&str] = &[
    "Deadline",
    "crate::Deadline",
    "Moment",
    "crate::Moment",
    "OperationDeadline",
    "crate::consumer::group_position",
    "GroupPositionBootstrapMachine",
    "Byte",
    "Bytes",
    "u8",
    "Callback",
    "Clock",
    "Future",
    "Runtime",
    "async",
    "kafka_client_engine",
    "kafka_driver",
    "kafka_wire",
    "std::future",
    "std::time",
];

#[test]
fn checked_in_retirement_policy_is_exact() {
    let root = workspace_root();
    let config = load_config(&root);
    let shared_error = read(&root.join(SHARED_ERROR));
    for retirement_only in ["AssignmentEpochMismatch", "EffectAllocationFailed"] {
        assert!(
            !shared_error.contains(retirement_only),
            "{retirement_only} must not expand the shared assigned-consumer error"
        );
    }
    for (production, test) in MIRRORS {
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == *production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, *test);
    }
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear owner");
        assert_eq!(rules[0].path, *path);
    }
    let assignment = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerMachine" && rule.field == "assignment")
        .collect::<Vec<_>>();
    assert_eq!(assignment.len(), 1, "assignment needs one mutation owner");
    assert!(
        assignment[0]
            .allowed_paths
            .iter()
            .any(|path| path == TRANSITION),
        "assignment omits the retirement transition"
    );
    let next_epoch = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerMachine" && rule.field == "next_epoch")
        .collect::<Vec<_>>();
    assert_eq!(next_epoch.len(), 1, "next_epoch needs one mutation owner");
    assert!(
        !next_epoch[0]
            .allowed_paths
            .iter()
            .any(|path| path == TRANSITION),
        "retirement must not spend or replace assignment epochs"
    );
    for root in [MODEL, TRANSITION] {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == root)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{root} needs one capability rule");
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            FORBIDDEN
        );
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn fixture_rejects_cloneable_retirement_and_foreign_assignment_mutation() {
    let (root, files) = fixture_files("consumer_assignment_retirement_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = [MutationOwner {
        owner_type: "AssignedConsumerMachine".into(),
        field: "assignment".into(),
        allowed_paths: Vec::new(),
    }];
    let violations = mutation_violations(&root, &files, &mutations);
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs") && violation.contains("assignment")
    }));
}

#[test]
fn fixture_rejects_deadline_bootstrap_bytes_runtime_and_transport_capabilities() {
    let (root, _files) = fixture_files("consumer_assignment_retirement_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for forbidden in FORBIDDEN {
        let expected = format!("forbidden capability {forbidden} through");
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(&expected)),
            "capability detector missed {forbidden}: {violations:?}"
        );
    }
}
