//! Executable ownership boundary for classic Join, Sync, and Heartbeat protocol facts.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, capability_violations, fixture_files,
    linear_violations, load_config, method_capability_violations, read, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/protocol/consumer/classic_group";
const COMPOSITION_ROOT: &str = "crates/kafka-client-engine/src/consumer/group";
const MODEL: &str = "crates/kafka-client-engine/src/protocol/consumer/classic_group/model.rs";
const JOIN_REQUEST: &str =
    "crates/kafka-client-engine/src/protocol/consumer/classic_group/join_request.rs";
const SYNC_REQUEST: &str =
    "crates/kafka-client-engine/src/protocol/consumer/classic_group/sync_request.rs";
const HEARTBEAT_REQUEST: &str =
    "crates/kafka-client-engine/src/protocol/consumer/classic_group/heartbeat_request.rs";
const JOIN_CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/join_group_calls.rs";
const SYNC_CALLS: &str =
    "crates/kafka-client-engine/src/driver/rpc/classic_group/sync_group_calls.rs";
const LINEAR: &[(&str, &str)] = &[
    ("ClassicJoinedMember", MODEL),
    ("ClassicJoinedRole", MODEL),
    ("ClassicJoinedGroup", MODEL),
    ("ClassicJoinOutcome", MODEL),
    ("ClassicSyncMember", MODEL),
    ("ClassicSyncTopic", MODEL),
    ("NamedAssignmentPartition", MODEL),
    ("ClassicSyncOutcome", MODEL),
    ("PreparedClassicJoinGroupRequest", JOIN_REQUEST),
    ("PreparedClassicSyncGroupRequest", SYNC_REQUEST),
    ("PreparedClassicHeartbeatRequest", HEARTBEAT_REQUEST),
];
const METHODS: &[(&str, &str)] = &[
    ("into_sync_assignments", SYNC_REQUEST),
    ("into_generated_join_group_request", JOIN_CALLS),
    ("into_generated_sync_group_request", SYNC_CALLS),
    (
        "into_generated_heartbeat_request",
        "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_calls.rs",
    ),
];
const RAW_REQUESTS: &[&str] = &["HeartbeatRequest", "JoinGroupRequest", "SyncGroupRequest"];
const FORBIDDEN: &[&str] = &[
    "crate::admin",
    "crate::clock",
    "crate::consumer",
    "crate::driver",
    "crate::producer",
    "crate::transaction",
    "kafka_driver",
    "kafka_wire_records",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "async",
    "Condvar",
    "Instant::now",
    "Mutex",
    "Retry",
    "RwLock",
    "Transport",
];

#[test]
fn checked_in_classic_group_protocol_policy_is_exact() {
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
    let rules = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(rules.len(), 1);
    assert_eq!(
        rules[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    assert!(rules[0].allow.is_empty());
    let composition = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == COMPOSITION_ROOT)
        .collect::<Vec<_>>();
    assert_eq!(composition.len(), 1);
    assert_eq!(
        composition[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        RAW_REQUESTS
    );
    assert!(composition[0].allow.is_empty());
    for (method, path) in METHODS {
        let methods = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(methods.len(), 1, "{method} needs one method rule");
        assert_eq!(methods[0].allowed_paths, [*path]);
    }
}

#[test]
fn fixture_rejects_cloneable_protocol_owners() {
    let (root, files) = fixture_files("consumer_classic_group_protocol");
    let rules = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &rules);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}

#[test]
fn fixture_rejects_policy_capabilities_and_plan_transfer_theft() {
    let (root, _) = fixture_files("consumer_classic_group_protocol");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(violations.iter().any(|violation| {
            violation.contains("capability_intruder.rs") && violation.contains(capability)
        }));
    }
    for (method, _path) in METHODS {
        let rules = [MethodCapabilityRule {
            root: "src".into(),
            method: (*method).into(),
            allowed_paths: vec!["src/method_owner.rs".into()],
        }];
        let violations = method_capability_violations(&root, &rules);
        assert!(violations.iter().any(|violation| {
            violation.contains("method_intruder.rs") && violation.contains(method)
        }));
        assert!(
            !violations
                .iter()
                .any(|violation| violation.contains("method_owner.rs"))
        );
    }

    let raw = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: RAW_REQUESTS.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for request in RAW_REQUESTS {
        assert!(raw.iter().any(|violation| {
            violation.contains("raw_request_intruder.rs") && violation.contains(request)
        }));
    }
}

#[test]
fn builders_and_submission_permits_bind_the_opaque_request_owners() {
    let root = workspace_root();
    for (request_path, calls_path, prepared, generated, consume) in [
        (
            JOIN_REQUEST,
            JOIN_CALLS,
            "PreparedClassicJoinGroupRequest",
            "JoinGroupRequest",
            "into_generated_join_group_request",
        ),
        (
            SYNC_REQUEST,
            SYNC_CALLS,
            "PreparedClassicSyncGroupRequest",
            "SyncGroupRequest",
            "into_generated_sync_group_request",
        ),
        (
            HEARTBEAT_REQUEST,
            "crates/kafka-client-engine/src/driver/rpc/classic_group/heartbeat_calls.rs",
            "PreparedClassicHeartbeatRequest",
            "HeartbeatRequest",
            "into_generated_heartbeat_request",
        ),
    ] {
        let request = compact(&read(&root.join(request_path)));
        assert!(request.contains(&format!(")->Result<{prepared},")));
        assert!(!request.contains(&format!(")->Result<{generated},")));
        let calls = compact(&read(&root.join(calls_path)));
        assert!(calls.contains(&format!("request:{prepared},")));
        assert!(calls.contains(&format!("request.{consume}()")));
    }
}

fn compact(source: &str) -> String {
    source.split_whitespace().collect()
}
