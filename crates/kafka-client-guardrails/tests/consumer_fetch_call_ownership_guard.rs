//! Registration and negative evidence for tracked partition Fetch ownership.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const LINEAR_OWNERS: &[(&str, &str)] = &[
    (
        "PartitionFetchRequest",
        "crates/kafka-client-engine/src/driver/rpc/fetch/admission.rs",
    ),
    (
        "FetchAdmissionFailure",
        "crates/kafka-client-engine/src/driver/rpc/fetch/admission.rs",
    ),
    (
        "FetchCallAdmission",
        "crates/kafka-client-engine/src/driver/rpc/fetch/admission.rs",
    ),
    (
        "FetchCallPermit",
        "crates/kafka-client-engine/src/driver/rpc/fetch/calls.rs",
    ),
    (
        "TrackedFetchCall",
        "crates/kafka-client-engine/src/driver/rpc/fetch/calls.rs",
    ),
    (
        "SettledFetchCall",
        "crates/kafka-client-engine/src/driver/rpc/fetch/settlement.rs",
    ),
    (
        "TrackedFetchCalls",
        "crates/kafka-client-engine/src/driver/rpc/fetch/calls.rs",
    ),
    (
        "FetchTerminal",
        "crates/kafka-client-engine/src/driver/rpc/fetch/terminal.rs",
    ),
    (
        "FetchCompletionFailure",
        "crates/kafka-client-engine/src/driver/rpc/fetch/terminal.rs",
    ),
    (
        "PendingFetchConfirmation",
        "crates/kafka-client-engine/src/driver/rpc/fetch/settlement.rs",
    ),
    (
        "FetchRestoreFailure",
        "crates/kafka-client-engine/src/driver/rpc/fetch/settlement.rs",
    ),
    (
        "StaleFetchDrains",
        "crates/kafka-client-engine/src/driver/rpc/fetch/stale.rs",
    ),
    (
        "FetchRecovery",
        "crates/kafka-client-engine/src/driver/rpc/fetch/stale.rs",
    ),
];

const CALLS: &str = "crates/kafka-client-engine/src/driver/rpc/fetch/calls.rs";
const SETTLEMENT: &str = "crates/kafka-client-engine/src/driver/rpc/fetch/settlement.rs";
const SETTLEMENT_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/fetch/settlement_owner.rs";
const MUTATION_OWNERS: &[(&str, &str, &[&str])] = &[
    ("TrackedFetchCalls", "calls", &[CALLS, SETTLEMENT_OWNER]),
    ("TrackedFetchCalls", "settled", &[CALLS, SETTLEMENT_OWNER]),
    (
        "TrackedFetchCalls",
        "pending_confirmation",
        &[CALLS, SETTLEMENT_OWNER],
    ),
    (
        "TrackedFetchCalls",
        "completion_failure",
        &[CALLS, SETTLEMENT_OWNER],
    ),
    ("TrackedFetchCall", "request", &[CALLS, SETTLEMENT_OWNER]),
    ("SettledFetchCall", "terminal", &[SETTLEMENT]),
];
const OWNER_METHODS: &[&str] = &[
    "try_submit_fetch",
    "observe_fetch_control",
    "poll_fetch",
    "begin_fetch_settlement",
    "confirm_fetch_settlement",
    "restore_fetch_settlement",
    "confirm_stale_fetch",
    "recover_fetches_after_driver_shutdown",
];

#[test]
fn checked_in_fetch_call_owners_are_narrow_and_linear() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR_OWNERS {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }

    for (owner_type, field, allowed_paths) in MUTATION_OWNERS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths,
        );
    }
}

#[test]
fn checked_in_fetch_executor_methods_have_no_undeclared_caller() {
    let config = load_config(&workspace_root());
    for method in OWNER_METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method capability");
        assert!(
            rules[0].allowed_paths.is_empty(),
            "{method} must remain uncalled until the executor is registered"
        );
    }
}

#[test]
fn fixture_rejects_fetch_call_mutation_outside_the_owner() {
    let (root, files) = fixture_files("consumer_fetch_call_ownership");
    let rules = MUTATION_OWNERS
        .iter()
        .map(|(owner_type, field, _allowed_paths)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: vec!["src/mutation_owner.rs".into()],
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &rules);
    for (owner_type, field, _allowed_paths) in MUTATION_OWNERS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_clone_and_copy_for_every_fetch_call_owner() {
    let (root, files) = fixture_files("consumer_fetch_call_ownership");
    let rules = LINEAR_OWNERS
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &rules);
    for (owner_type, _path) in LINEAR_OWNERS {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}
