//! Linear ownership and capability ratchets for classic-group Fetch activation.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations,
    mutation_violations, read, workspace_root,
};

const ROOT: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch";
const ACTIVATION: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/activation.rs";
const OWNER: &str = "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner.rs";
const PREPARE: &str =
    "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/prepare.rs";
const ENTRY: &str = "crates/kafka-client-engine/src/consumer/group/registry_entry.rs";
const LINEAR: &[(&str, &str)] = &[
    ("ClassicGroupFetchBinding", ACTIVATION),
    ("ClassicGroupFetchActivation", ACTIVATION),
    ("ClassicGroupFetchActivationFault", ACTIVATION),
    ("ClassicGroupFetchActivationFailure", ACTIVATION),
    ("ClassicGroupFetchActivationError", ACTIVATION),
    ("ClassicGroupFetchOwner", OWNER),
];
const FAILURE_FORBIDDEN: &[&str] = &["panic!", "unreachable!", ".expect(", ".unwrap("];
const FORBIDDEN: &[&str] = &[
    "crate::clock",
    "crate::driver",
    "crate::protocol",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_core",
    "FetchAttemptDeadline",
    "OperationDeadline",
    "MonotonicClock",
    "DirectFetchExecutor",
    "AssignedTimers",
    "AssignedConsumerDelivery",
    "FetchDelivery",
    "FetchDeliveryStore",
    "std::future",
    "std::net",
    "std::thread",
    "std::time",
    "tokio",
    "async",
];
const METHODS: &[&str] = &[
    "prepare_classic_group_fetch_activation",
    "install_resolved_assignment",
];

fn source_token_violations(path: &str, source: &str, forbidden: &[&str]) -> Vec<String> {
    forbidden
        .iter()
        .filter(|token| source.contains(**token))
        .map(|token| format!("{path} contains forbidden source token {token}"))
        .collect()
}

#[test]
fn checked_in_group_fetch_activation_policy_is_exact() {
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
    for field in ["activation", "fault"] {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == "ClassicGroupFetchOwner" && rule.field == field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{field} needs one mutation rule");
        let expected = if field == "activation" {
            vec![OWNER]
        } else {
            vec![OWNER, PREPARE]
        };
        assert_eq!(rules[0].allowed_paths, expected);
    }
    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ACTIVATION)
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
    for method in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method && rule.root == ROOT)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method rule");
        assert_eq!(rules[0].allowed_paths, [OWNER]);
    }
}

#[test]
fn fixtures_reject_cloneable_owners_and_foreign_mutation() {
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
    for field in ["activation", "fault"] {
        let violations = mutation_violations(
            &root,
            &files,
            &[MutationOwner {
                owner_type: "ClassicGroupFetchOwner".into(),
                field: field.into(),
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
fn fixtures_reject_runtime_transport_and_activation_authority_theft() {
    let (root, _) = fixture_files("consumer_classic_group_fetch_activation");
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
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "missing fixture violation for {capability}: {violations:?}"
        );
    }
    let post_core_path = "src/post_core_intruder.rs";
    let post_core = read(&root.join(post_core_path));
    let violations = source_token_violations(post_core_path, &post_core, FAILURE_FORBIDDEN);
    for capability in FAILURE_FORBIDDEN {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("post_core_intruder.rs") && violation.contains(capability)
            }),
            "missing fixture violation for {capability}: {violations:?}"
        );
    }
    for method in METHODS {
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
fn sole_owner_preflights_installs_and_moves_ordered_effects_without_attempt_capture() {
    let root = workspace_root();
    let owner = read(&root.join(OWNER));
    assert!(
        source_token_violations(OWNER, &owner, FAILURE_FORBIDDEN).is_empty(),
        "activation owner may not abandon post-core owners through process failure"
    );
    for required in [
        "AssignedConsumerMachine::with_read_isolation",
        "prepare_classic_group_fetch_activation",
        "preflight_activation_capacity",
        "prepare_replacement",
        "install_resolved_assignment",
        "commit_event_claims",
        "transition.into_effects()",
        "ClassicGroupFetchBinding::new",
        "ClassicGroupFetchActivation::new",
    ] {
        assert!(owner.contains(required), "owner lost {required}");
    }
    for forbidden in [
        "FetchAttemptDeadline",
        "capture_for_fetch",
        "DriverOwner",
        ".submit(",
        ".poll(",
        "AssignedConsumerDelivery",
        "FetchDelivery",
        "FetchDeliveryStore",
    ] {
        assert!(
            !owner.contains(forbidden),
            "activation owner stole later capability {forbidden}"
        );
    }
    let entry = read(&root.join(ENTRY));
    assert_eq!(
        entry.matches("ClassicGroupFetchOwner::try_new()").count(),
        1
    );
}
