//! Capability, mutation, and linear-ownership ratchets for group Fetch preparation.

mod support;

use support::{
    CallCapabilityRule, CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    call_capability_violations, capability_violations, fixture_files, linear_violations,
    load_config, method_capability_violations, mutation_violations, workspace_root,
};

const ENGINE_ROOT: &str = "crates/kafka-client-engine/src";
const GROUP_FETCH_ROOT: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch";
const OWNER: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner.rs";
const CONTROL: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/control.rs";
const OWNER_OBSERVATION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner_observation.rs";
const MODEL: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/model.rs";
const PREPARE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/prepare.rs";
const RETIREMENT: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/retirement.rs";
const TURN: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/turn.rs";
const RECOVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/recovery.rs";
const DELIVERY: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/delivery.rs";
const FETCH_PREPARED: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/prepared.rs";
const COMMON_FORBIDDEN: &[&str] = &[
    "crate::driver",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "AssignedConsumerOwner",
    "PositionResolutionExecutor",
    "CompletionRegistry",
    "AssignedConsumerPort",
    "std::future",
    "std::net",
    "std::thread",
    "tokio",
    "async",
];
const PREPARE_FORBIDDEN: &[&str] = &[
    "crate::driver",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "kafka_wire_records",
    "AssignedConsumerOwner",
    "PositionResolutionExecutor",
    "CompletionRegistry",
    "AssignedConsumerPort",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "Instant",
    "tokio",
    "async",
];
const LINEAR: &[(&str, &str)] = &[
    ("ClassicGroupFetchOwnerFault", MODEL),
    ("ClassicGroupFetchPrepareFailure", PREPARE),
    ("PrepareFetchFailure", FETCH_PREPARED),
];
const MUTATIONS: &[(&str, &[&str])] = &[
    (
        "fault",
        &[
            OWNER,
            CONTROL,
            OWNER_OBSERVATION,
            PREPARE,
            RETIREMENT,
            TURN,
            RECOVERY,
            DELIVERY,
        ],
    ),
    (
        "effects",
        &[OWNER, CONTROL, PREPARE, RETIREMENT, TURN, DELIVERY],
    ),
    (
        "pending_fetches",
        &[OWNER, OWNER_OBSERVATION, PREPARE, TURN],
    ),
];
const CALLS: &[(&str, &[&str])] = &[
    (
        "DirectFetchExecutor::create_unbound",
        &[
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            OWNER,
        ],
    ),
    (
        "FetchAttemptDeadline::capture_for_fetch",
        &[
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            PREPARE,
        ],
    ),
    ("PreparedFetchExecution::new_retaining_attempt", &[PREPARE]),
    (
        "PartitionFetchRequest::from_fetch_ready_parts",
        &[FETCH_PREPARED],
    ),
];
const METHODS: &[(&str, &str, &[&str])] = &[
    (
        ENGINE_ROOT,
        "observe_effect",
        &[
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            PREPARE,
        ],
    ),
    (GROUP_FETCH_ROOT, "arm_fetch", &[PREPARE]),
    (GROUP_FETCH_ROOT, "observe_control", &[PREPARE]),
];

#[test]
fn checked_in_preparation_policy_is_exact() {
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
    for (field, expected) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "ClassicGroupFetchOwner" && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation rule");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *expected
        );
    }
    assert_capability(&config, OWNER, COMMON_FORBIDDEN);
    assert_capability(&config, MODEL, COMMON_FORBIDDEN);
    assert_capability(&config, PREPARE, PREPARE_FORBIDDEN);
    for (call, expected) in CALLS {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one call rule");
        assert_eq!(rules[0].root, ENGINE_ROOT);
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *expected
        );
    }
    for (root, method, expected) in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].root, *root);
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *expected
        );
    }
    for (production, test) in [
        (
            MODEL,
            "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/model_test.rs",
        ),
        (
            PREPARE,
            "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/prepare_ready_test.rs",
        ),
    ] {
        assert!(
            config
                .test_mirrors
                .iter()
                .any(|mirror| { mirror.production == production && mirror.test == test })
        );
    }
}

#[test]
fn fixtures_reject_duplicated_attempt_owners_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_classic_group_fetch_activation");
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
    for (field, _) in MUTATIONS {
        let violations = mutation_violations(
            &root,
            &files,
            &[MutationOwner {
                owner_type: "ClassicGroupFetchOwner".into(),
                field: (*field).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(&format!("ClassicGroupFetchOwner.{field}"))
        }));
    }
}

#[test]
fn fixtures_reject_raw_runtime_and_constructor_authority() {
    let (root, _) = fixture_files("consumer_classic_group_fetch_activation");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: PREPARE_FORBIDDEN
                .iter()
                .map(|value| (*value).into())
                .collect(),
            allow: Vec::new(),
        }],
    );
    for capability in PREPARE_FORBIDDEN {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "missing fixture violation for {capability}: {violations:?}"
        );
    }
    for (call, _) in CALLS {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: (*call).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(violations.iter().any(|violation| {
            violation.contains("constructor_intruder.rs") && violation.contains(call)
        }));
    }
    for (_, method, _) in METHODS {
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

fn assert_capability(config: &support::GuardConfig, root: &str, expected: &[&str]) {
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
        expected
    );
}
