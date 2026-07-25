//! Exact ownership boundary for deterministic classic heartbeat cadence.

mod support;

use support::{
    CallCapabilityRule, LinearOwner, MutationOwner, call_capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

const ROOT: &str = "crates/kafka-client-core/src/consumer/classic_group";
const HEARTBEAT: &str = "crates/kafka-client-core/src/consumer/classic_group/heartbeat.rs";
const STATE: &str = "crates/kafka-client-core/src/consumer/classic_group/heartbeat_state.rs";
const TRANSITION: &str =
    "crates/kafka-client-core/src/consumer/classic_group/heartbeat_transition.rs";
const MACHINE: &str = "crates/kafka-client-core/src/consumer/classic_group/machine.rs";
const MIRRORS: &[(&str, &str)] = &[
    (
        HEARTBEAT,
        "crates/kafka-client-core/src/consumer/classic_group/heartbeat_test.rs",
    ),
    (
        STATE,
        "crates/kafka-client-core/src/consumer/classic_group/heartbeat_state_test.rs",
    ),
    (
        TRANSITION,
        "crates/kafka-client-core/src/consumer/classic_group/heartbeat_transition_test.rs",
    ),
];
const CALLS: &[(&str, &str)] = &[("ClassicHeartbeatAttempt::first", STATE)];

#[test]
fn checked_in_heartbeat_core_policy_is_exact() {
    let config = load_config(&workspace_root());
    let linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == "ClassicHeartbeatState")
        .collect::<Vec<_>>();
    assert_eq!(linear.len(), 1);
    assert_eq!(linear[0].path, STATE);

    let mutation = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "ClassicHeartbeatState" && rule.field == "phase")
        .collect::<Vec<_>>();
    assert_eq!(mutation.len(), 1);
    assert_eq!(mutation[0].allowed_paths, [STATE]);

    for (call, path) in CALLS {
        let rules = config
            .call_capabilities
            .iter()
            .filter(|rule| rule.call == *call)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{call} needs one constructor rule");
        assert_eq!(rules[0].root, ROOT);
        assert_eq!(rules[0].allowed_paths, [*path]);
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
    assert!(
        compact(
            &std::fs::read_to_string(workspace_root().join(STATE))
                .unwrap_or_else(|error| panic!("read heartbeat state: {error}"))
        )
        .contains("ClassicHeartbeatSchedule::new(")
    );
    assert!(
        compact(
            &std::fs::read_to_string(workspace_root().join(MACHINE))
                .unwrap_or_else(|error| panic!("read classic group machine: {error}"))
        )
        .contains("ClassicHeartbeatState::new(heartbeat_policy)")
    );
}

fn compact(source: &str) -> String {
    source.split_whitespace().collect()
}

#[test]
fn fixture_rejects_clone_mutation_and_foreign_construction() {
    let (root, files) = fixture_files("consumer_classic_group_heartbeat_core");
    let linear = linear_violations(
        &root,
        &files,
        &[LinearOwner {
            owner_type: "ClassicHeartbeatState".into(),
            path: "src/linear_intruder.rs".into(),
        }],
    );
    for derived in ["derives Clone", "derives Copy"] {
        assert!(
            linear.iter().any(|violation| {
                violation.contains("ClassicHeartbeatState") && violation.contains(derived)
            }),
            "linear fixture missed {derived}: {linear:?}"
        );
    }

    let mutations = mutation_violations(
        &root,
        &files,
        &[MutationOwner {
            owner_type: "ClassicHeartbeatState".into(),
            field: "phase".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(mutations.iter().any(|violation| {
        violation.contains("mutation_intruder.rs")
            && violation.contains("ClassicHeartbeatState.phase")
    }));

    for (call, _path) in CALLS {
        let violations = call_capability_violations(
            &root,
            &[CallCapabilityRule {
                root: "src".into(),
                call: (*call).into(),
                allowed_paths: Vec::new(),
            }],
        );
        assert!(
            violations.iter().any(|violation| {
                violation.contains("constructor_intruder.rs") && violation.contains(call)
            }),
            "constructor fixture missed {call}: {violations:?}"
        );
    }
}
