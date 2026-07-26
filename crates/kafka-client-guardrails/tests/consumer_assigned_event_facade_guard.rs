//! Exact linearity and test-mirror ratchets for assigned event observation.

mod support;

use support::{LinearOwner, fixture_files, linear_violations, load_config, workspace_root};

const MIRRORS: &[(&str, &str)] = &[
    (
        "crates/kafka-client/src/bridge/consumer/next_event.rs",
        "crates/kafka-client/src/bridge/consumer/next_event_test.rs",
    ),
    (
        "crates/kafka-client/src/bridge/consumer/next_event_result.rs",
        "crates/kafka-client/src/bridge/consumer/next_event_result_test.rs",
    ),
    (
        "crates/kafka-client/src/consumer/assigned_next_event.rs",
        "crates/kafka-client/src/consumer/assigned_next_event_test.rs",
    ),
];
const LINEAR: &[(&str, &str)] = &[
    (
        "AssignedConsumerNextEvent",
        "crates/kafka-client/src/bridge/consumer/next_event.rs",
    ),
    (
        "NextAssignedEvent",
        "crates/kafka-client/src/consumer/assigned_next_event.rs",
    ),
];

#[test]
fn checked_in_observers_and_sibling_tests_are_exact() {
    let config = load_config(&workspace_root());
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
            .filter(|rule| rule.owner_type == *owner_type && rule.path == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one owner at {path}");
    }
}

#[test]
fn fixture_rejects_clone_and_copy_for_both_observation_layers() {
    let (root, files) = fixture_files("consumer_assigned_event_facade");
    let rules = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &rules);

    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}
