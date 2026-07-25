//! Executable ownership and capability boundaries for group offset commit execution.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, rust_files, workspace_root,
};

const CALL_PATH: &str = "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_calls.rs";
const RECOVERY_PATH: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_recovery.rs";
const SETTLEMENT_PATH: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_settlement.rs";
const SETTLEMENT_OWNER_PATH: &str =
    "crates/kafka-client-engine/src/driver/rpc/group_offset_commit_settlement_owner.rs";
const SNAPSHOT_PATH: &str =
    "crates/kafka-client-engine/src/protocol/consumer/group_offset_commit/snapshot.rs";
const LINEAR_OWNERS: [(&str, &str); 16] = [
    (
        "ClassicGroupCommitSession",
        "crates/kafka-client-engine/src/protocol/consumer/group_offset_commit/session.rs",
    ),
    (
        "PreparedGroupOffsetCommit",
        "crates/kafka-client-engine/src/protocol/consumer/group_offset_commit/model.rs",
    ),
    (
        "GroupOffsetCommitEntryReservation",
        "crates/kafka-client-engine/src/protocol/consumer/group_offset_commit/entry_reservation.rs",
    ),
    (
        "GroupOffsetCommitResultReservation",
        "crates/kafka-client-engine/src/protocol/consumer/group_offset_commit/result_reservation.rs",
    ),
    (
        "GroupOffsetCommitPreparationError",
        "crates/kafka-client-engine/src/protocol/consumer/group_offset_commit/preparation.rs",
    ),
    ("GroupOffsetCommitCallPermit", CALL_PATH),
    ("TrackedGroupOffsetCommitCall", CALL_PATH),
    ("TrackedGroupOffsetCommitCalls", CALL_PATH),
    ("GroupOffsetCommitAdmissionFailure", RECOVERY_PATH),
    ("GroupOffsetCommitCompletionFailure", RECOVERY_PATH),
    ("GroupOffsetCommitCompletionRecovery", RECOVERY_PATH),
    ("RecoveredGroupOffsetCommitSettlement", RECOVERY_PATH),
    ("GroupOffsetCommitShutdownRecovery", RECOVERY_PATH),
    ("SettledGroupOffsetCommitCall", SETTLEMENT_PATH),
    ("PendingGroupOffsetCommitConfirmation", SETTLEMENT_PATH),
    ("GroupOffsetCommitRestoreFailure", SETTLEMENT_PATH),
];
const MUTATIONS: [(&str, &str); 4] = [
    ("TrackedGroupOffsetCommitCalls", "calls"),
    ("TrackedGroupOffsetCommitCalls", "settled"),
    ("TrackedGroupOffsetCommitCalls", "pending_confirmation"),
    ("TrackedGroupOffsetCommitCalls", "completion_failure"),
];

#[test]
fn live_group_commit_owners_are_registered_and_linear() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    for (owner_type, path) in LINEAR_OWNERS {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear owner");
        assert_eq!(rules[0].path, path);
    }
    for (owner_type, field) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        let expected = match (owner_type, field) {
            ("TrackedGroupOffsetCommitCalls", "calls" | "completion_failure") => vec![CALL_PATH],
            ("TrackedGroupOffsetCommitCalls", "settled" | "pending_confirmation") => {
                vec![CALL_PATH, SETTLEMENT_OWNER_PATH]
            }
            _ => panic!("unexpected mutation owner"),
        };
        assert_eq!(rules[0].allowed_paths, expected);
    }
    let files = rust_files(&workspace, &config);
    let violations = linear_violations(&workspace, &files, &config.linear_owners);
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn route_token_discard_is_confined_to_settlement_owner() {
    let config = load_config(&workspace_root());
    let rule = config
        .method_capabilities
        .iter()
        .find(|rule| rule.method == "confirm_group_commit_route_token")
        .unwrap_or_else(|| panic!("route-token discard needs one method capability"));
    assert_eq!(rule.allowed_paths, [SETTLEMENT_OWNER_PATH]);

    let (root, _) = fixture_files("consumer_group_offset_commit_ownership");
    let fixture_rule = [MethodCapabilityRule {
        root: "src".into(),
        method: "confirm_group_commit_route_token".into(),
        allowed_paths: vec!["src/settlement_owner.rs".into()],
    }];
    let violations = method_capability_violations(&root, &fixture_rule);
    assert!(violations.iter().any(|violation| {
        violation.contains("settlement_intruder.rs")
            && violation.contains("confirm_group_commit_route_token")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("settlement_owner.rs"))
    );
}

#[test]
fn entry_reservation_transfer_is_confined_to_snapshot_owner() {
    let config = load_config(&workspace_root());
    for method in ["into_entries", "recover_entries"] {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method capability");
        assert_eq!(rules[0].allowed_paths, [SNAPSHOT_PATH]);
    }

    let (root, _) = fixture_files("consumer_group_offset_commit_ownership");
    let fixture_rules = ["into_entries", "recover_entries"].map(|method| MethodCapabilityRule {
        root: "src".into(),
        method: method.into(),
        allowed_paths: vec!["src/entry_reservation_owner.rs".into()],
    });
    let violations = method_capability_violations(&root, &fixture_rules);
    for method in ["into_entries", "recover_entries"] {
        assert!(violations.iter().any(|violation| {
            violation.contains("entry_reservation_intruder.rs") && violation.contains(method)
        }));
    }
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("entry_reservation_owner.rs"))
    );
}

#[test]
fn fixtures_reject_duplicated_and_foreign_mutated_owners() {
    let (root, files) = fixture_files("consumer_group_offset_commit_ownership");
    let linear = LINEAR_OWNERS.map(|(owner_type, _)| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let linear_violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR_OWNERS {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(linear_violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = MUTATIONS.map(|(owner_type, field)| MutationOwner {
        owner_type: owner_type.into(),
        field: field.into(),
        allowed_paths: vec!["src/mutation_owner.rs".into()],
    });
    let mutation_violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field) in MUTATIONS {
        assert!(mutation_violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_runtime_and_invalidation_capabilities() {
    let (root, _) = fixture_files("consumer_group_offset_commit_ownership");
    let forbidden = [
        "std::future",
        "std::net",
        "std::thread",
        "std::time",
        "Retry",
        "invalidate",
        "async",
    ];
    let rules = [CapabilityRule {
        root: "src".into(),
        forbidden: forbidden.iter().map(|value| (*value).into()).collect(),
        allow: vec![],
    }];
    let violations = capability_violations(&root, &rules);
    for capability in forbidden {
        assert!(violations.iter().any(|violation| {
            violation.contains("capability_intruder.rs") && violation.contains(capability)
        }));
    }
}
