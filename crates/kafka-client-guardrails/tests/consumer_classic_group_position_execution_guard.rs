//! Ownership, mutation, capability, and hosted-call ratchets for group positions.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, read, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_position";
const ACTIVATION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/activation.rs";
const STATE: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_position/state.rs";
const EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/state_execution.rs";
const MEMBERSHIP_LOCAL: &str =
    "crates/kafka-client-engine/src/consumer/group/registry_membership/local.rs";
const HOST: &str = "crates/kafka-client-engine/src/consumer/group/registry_host.rs";
const RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/recovery.rs";
const CLOSE: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_position/close.rs";
const REGISTRY_RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_recovery.rs";
const REGISTRY_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_settlement.rs";
const REGISTRY_SUBMISSION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_submission.rs";
const POSITION_TURN: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_position/registry_turn.rs";
const LINEAR: &[(&str, &str)] = &[
    ("ClassicGroupPositionPrepared", STATE),
    ("ClassicGroupPositionHandoff", STATE),
    ("ClassicGroupPositionDriverOwned", STATE),
    ("ClassicGroupPositionCompleted", STATE),
    ("ClassicGroupPositionConfirmationPending", STATE),
    ("ClassicGroupPositionExecutionState", EXECUTION),
    ("ClassicGroupPositionExecution", EXECUTION),
];
const FORBIDDEN: &[&str] = &[
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "std::future",
    "std::net",
    "std::thread",
    "tokio",
    "async",
];
const METHODS: &[(&str, &[&str])] = &[
    (
        "try_submit_group_position_offset_fetch",
        &[REGISTRY_SUBMISSION],
    ),
    ("begin_handoff", &[REGISTRY_SUBMISSION]),
    ("restore_prepared", &[REGISTRY_SUBMISSION]),
    ("confirm_driver_owned", &[REGISTRY_SUBMISSION]),
    ("finish_driver_rejected", &[REGISTRY_SUBMISSION]),
    ("apply_raw_terminal", &[RECOVERY, REGISTRY_SETTLEMENT]),
    ("confirm_terminal_settlement", &[REGISTRY_SETTLEMENT]),
    ("close_position_if_local", &[CLOSE, MEMBERSHIP_LOCAL]),
    ("expire_prepared_if_due", &[REGISTRY_SUBMISSION]),
    ("recover_key_after_driver_shutdown", &[REGISTRY_RECOVERY]),
    (
        "recover_terminal_after_driver_shutdown",
        &[REGISTRY_RECOVERY],
    ),
    (
        "recover_confirmation_after_driver_shutdown",
        &[RECOVERY, REGISTRY_RECOVERY],
    ),
];

#[test]
fn checked_in_position_execution_policy_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    let mutations = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "ClassicGroupPositionExecution" && rule.field == "state")
        .collect::<Vec<_>>();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].allowed_paths, [EXECUTION]);

    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
    assert_eq!(
        capabilities[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    for (method, paths) in METHODS {
        let methods = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(methods.len(), 1, "{method} needs one method rule");
        assert_eq!(
            methods[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *paths
        );
    }
}

#[test]
fn fixtures_reject_cloneable_owners_and_foreign_state_mutation() {
    let (root, files) = fixture_files("consumer_classic_group_position_execution");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
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
    let violations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "ClassicGroupPositionExecution".into(),
            field: "state".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(violations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("ClassicGroupPositionExecution.state")
    }));
}

#[test]
fn fixtures_reject_raw_runtime_and_hosted_method_theft() {
    let (root, _) = fixture_files("consumer_classic_group_position_execution");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(violations.iter().any(|violation| {
            violation.contains("capability_intruder.rs") && violation.contains(capability)
        }));
    }
    for (method, _) in METHODS {
        let violations = method_capability_violations(
            &root,
            &[MethodCapabilityRule {
                root: "src".into(),
                method: (*method).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
    }
}

#[test]
fn host_order_is_commit_then_membership_then_one_position_action() {
    let root = workspace_root();
    let host = read(&root.join(HOST));
    let commit = host
        .find(".offset_commits\n            .turn")
        .unwrap_or_else(|| panic!("offset commit turn expected"));
    let membership = host
        .find(".turn_membership")
        .unwrap_or_else(|| panic!("membership turn expected"));
    let position = host
        .find(".turn_position")
        .unwrap_or_else(|| panic!("position turn expected"));
    assert!(commit < membership && membership < position);

    let position_turn = read(&root.join(POSITION_TURN));
    let settlement = position_turn
        .find("settle_one_classic_group_position")
        .unwrap_or_else(|| panic!("position settlement expected"));
    let submission = position_turn
        .find("submit_one_classic_group_position")
        .unwrap_or_else(|| panic!("position submission expected"));
    assert!(settlement < submission);
}

#[test]
fn activation_preserves_position_time_without_claiming_fetch_attempt_time() {
    let root = workspace_root();
    let activation = read(&root.join(ACTIVATION));
    for required in [
        "prepare_classic_group_fetch_activation",
        "InstallResolvedAssignment::new",
        "completed.observed_at()",
        "throttle_ticks",
    ] {
        assert!(
            activation.contains(required),
            "activation handoff lost {required}"
        );
    }
    for forbidden in [
        "FetchAttemptDeadline",
        "capture_for_fetch",
        "MonotonicClock",
        "OperationDeadline",
        "DeadlineCapture",
        "Instant",
    ] {
        assert!(
            !activation.contains(forbidden),
            "position activation stole later Fetch time capability {forbidden}"
        );
    }
}
