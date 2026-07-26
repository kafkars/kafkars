//! Exact ownership and capability boundaries for group position RPC execution.

mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, rust_files, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch";
const ADMISSION: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/admission.rs";
const CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/calls.rs";
const KEY: &str = "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/key.rs";
const RECOVERY: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/recovery.rs";
const SETTLEMENT: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/settlement.rs";
const SETTLEMENT_OWNER: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/settlement_owner.rs";
const TERMINAL: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_position_offset_fetch/terminal.rs";
const LINEAR: &[(&str, &str)] = &[
    ("GroupPositionOffsetFetchKey", KEY),
    ("GroupPositionOffsetFetchAccepted", ADMISSION),
    ("GroupPositionOffsetFetchReturn", ADMISSION),
    ("GroupPositionOffsetFetchAdmissionFailure", ADMISSION),
    ("GroupPositionOffsetFetchAdmission", ADMISSION),
    ("TrackedGroupPositionOffsetFetchCall", CALLS),
    ("TrackedGroupPositionOffsetFetchCalls", CALLS),
    ("GroupPositionOffsetFetchTerminal", TERMINAL),
    ("SettledGroupPositionOffsetFetchCall", SETTLEMENT),
    ("PendingGroupPositionOffsetFetchConfirmation", SETTLEMENT),
    ("GroupPositionOffsetFetchConfirmationFailure", SETTLEMENT),
    ("GroupPositionOffsetFetchRestoreFailure", SETTLEMENT),
    ("GroupPositionOffsetFetchCompletionFailure", RECOVERY),
    ("GroupPositionOffsetFetchCompletionRecovery", RECOVERY),
    ("GroupPositionOffsetFetchShutdownRecovery", RECOVERY),
];
const MUTATIONS: &[(&str, &[&str])] = &[
    ("calls", &[CALLS, SETTLEMENT_OWNER]),
    ("settled", &[CALLS, SETTLEMENT_OWNER]),
    ("pending_confirmation", &[CALLS, SETTLEMENT_OWNER]),
    ("completion_failure", &[CALLS, SETTLEMENT_OWNER]),
];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::operation",
    "Callback",
    "Executor",
    "Instant::now",
    "Retry",
    "TrafficClass::LongPoll",
    "async",
    "invalidate",
    "std::net",
    "std::thread",
    "tokio",
];

#[test]
fn checked_in_rpc_owners_and_capability_policy_are_exact() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    for (field, paths) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| {
                rule.owner_type == "TrackedGroupPositionOffsetFetchCalls" && rule.field == *field
            })
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation rule");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *paths
        );
    }
    let rules = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    for forbidden in FORBIDDEN {
        assert!(rules[0].forbidden.iter().any(|value| value == forbidden));
    }
    let files = rust_files(&workspace, &config);
    let violations = linear_violations(&workspace, &files, &config.linear_owners);
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn fixtures_reject_cloneable_owners_and_foreign_registry_mutation() {
    let (root, files) = fixture_files("consumer_group_position_offset_fetch_rpc");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
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

    let mutations = MUTATIONS
        .iter()
        .map(|(field, _)| MutationOwner {
            owner_type: "TrackedGroupPositionOffsetFetchCalls".to_owned(),
            field: (*field).to_owned(),
            allowed_paths: vec!["src/mutation_owner.rs".to_owned()],
        })
        .collect::<Vec<_>>();
    let violations = mutation_violations(&root, &files, &mutations);
    for (field, _) in MUTATIONS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs") && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_admin_generic_runtime_and_route_capability_theft() {
    let (root, _) = fixture_files("consumer_group_position_offset_fetch_rpc");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: FORBIDDEN.iter().map(|value| (*value).to_owned()).collect(),
        allow: Vec::new(),
    }];
    let violations = capability_violations(&root, &rules);
    for capability in FORBIDDEN {
        assert!(violations.iter().any(|violation| {
            violation.contains("capability_intruder.rs") && violation.contains(capability)
        }));
    }
}
