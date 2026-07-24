//! Registration and negative evidence for shared-admin completion ownership.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const LINEAR_OWNERS: [(&str, &str); 7] = [
    (
        "PublishTicket",
        "crates/kafka-client-engine/src/completion/publish_ticket.rs",
    ),
    (
        "SharedNotifier",
        "crates/kafka-client-engine/src/completion/shared_notifier.rs",
    ),
    (
        "SharedPublishPort",
        "crates/kafka-client-engine/src/completion/shared_notifier.rs",
    ),
    (
        "SharedNotificationPort",
        "crates/kafka-client-engine/src/completion/shared_notifier.rs",
    ),
    (
        "AdminCompletionNotifier",
        "crates/kafka-client-engine/src/admin/completion.rs",
    ),
    (
        "AdminCompletionPorts",
        "crates/kafka-client-engine/src/admin/completion.rs",
    ),
    (
        "AdminPublishTicket",
        "crates/kafka-client-engine/src/admin/completion.rs",
    ),
];

#[test]
fn shared_admin_completion_owners_are_registered_exactly_once() {
    let config = load_config(&workspace_root());
    let worker_rules = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "AdminCompletionNotifier" && rule.field == "worker")
        .collect::<Vec<_>>();
    assert_eq!(worker_rules.len(), 1);
    assert_eq!(
        worker_rules[0].allowed_paths,
        ["crates/kafka-client-engine/src/admin/completion.rs"]
    );

    for (owner_type, path) in LINEAR_OWNERS {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} must have one linear owner");
        assert_eq!(rules[0].path, path);
    }
}

#[test]
fn shared_admin_completion_mutation_fixture_is_rejected() {
    let (root, files) = fixture_files("shared_admin_completion_ownership");
    let rules = [MutationOwner {
        owner_type: "AdminCompletionNotifier".into(),
        field: "worker".into(),
        allowed_paths: vec!["src/mutation_owner.rs".into()],
    }];
    let violations = mutation_violations(&root, &files, &rules);
    assert!(violations.iter().any(|value| {
        value.contains("mutation_intruder.rs")
            && value.contains("AdminCompletionNotifier")
            && value.contains("worker")
    }));
}

#[test]
fn shared_admin_completion_linear_fixture_rejects_clone_and_copy() {
    let (root, files) = fixture_files("shared_admin_completion_ownership");
    let rules = LINEAR_OWNERS.map(|(owner_type, _path)| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &rules);

    for (owner_type, _path) in LINEAR_OWNERS {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(
                violations
                    .iter()
                    .any(|value| value.contains(owner_type) && value.contains(derived)),
                "linear detector missed {derived} for {owner_type}: {violations:?}"
            );
        }
    }
}
