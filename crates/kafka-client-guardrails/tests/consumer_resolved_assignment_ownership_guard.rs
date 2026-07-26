//! Ownership and capability ratchets for deadline-free resolved assignment install.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, read, workspace_root,
};

const SHARED_ERROR: &str = "crates/kafka-client-core/src/consumer/error.rs";
const MODEL: &str = "crates/kafka-client-core/src/consumer/resolved_assignment.rs";
const INSTALL: &str = "crates/kafka-client-core/src/consumer/resolved_assignment_install.rs";
const MIRRORS: &[(&str, &str)] = &[
    (
        MODEL,
        "crates/kafka-client-core/src/consumer/resolved_assignment_test.rs",
    ),
    (
        INSTALL,
        "crates/kafka-client-core/src/consumer/resolved_assignment_install_test.rs",
    ),
];
const LINEAR: &[(&str, &str)] = &[
    ("InstallResolvedAssignment", MODEL),
    ("InstallResolvedAssignmentError", MODEL),
    ("PreparedResolvedAssignment", INSTALL),
];
const FORBIDDEN: &[&str] = &[
    "Deadline",
    "crate::Deadline",
    "crate::consumer::group_position",
    "GroupPositionBootstrapMachine",
    "PositionResolution",
    "StartPosition",
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
fn checked_in_resolved_assignment_policy_is_exact() {
    let root = workspace_root();
    let config = load_config(&root);
    let shared_error = read(&root.join(SHARED_ERROR));
    for resolved_only in [
        "ResolvedAssignmentOutOfOrder",
        "AssignmentAllocationFailed",
        "ResolvedAssignmentEpochMismatch",
        "InitialFetchThrottleDeadlineOverflow",
    ] {
        assert!(
            !shared_error.contains(resolved_only),
            "{resolved_only} must not expand the shared Assign error"
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
    for field in ["next_epoch", "assignment"] {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "AssignedConsumerMachine" && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation owner");
        assert!(
            rules[0].allowed_paths.iter().any(|path| path == INSTALL),
            "{field} omits the resolved install owner"
        );
    }
    for root in [MODEL, INSTALL] {
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
fn fixture_rejects_cloneable_inputs_and_foreign_machine_mutation() {
    let (root, files) = fixture_files("consumer_resolved_assignment_ownership");
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

    let mutations = ["next_epoch", "assignment"].map(|field| MutationOwner {
        owner_type: "AssignedConsumerMachine".into(),
        field: field.into(),
        allowed_paths: Vec::new(),
    });
    let violations = mutation_violations(&root, &files, &mutations);
    for field in ["next_epoch", "assignment"] {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_deadline_bootstrap_runtime_and_transport_capabilities() {
    let (root, _files) = fixture_files("consumer_resolved_assignment_ownership");
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
