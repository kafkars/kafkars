//! Registration and negative evidence for incremental configuration engine ownership.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const HOST_PATH: &str = "crates/kafka-client-engine/src/admin/alter_configs/host.rs";
const ADMISSION_PATH: &str = "crates/kafka-client-engine/src/admin/alter_configs/host/admission.rs";
const TERMINAL_PATH: &str = "crates/kafka-client-engine/src/admin/alter_configs/host/terminal.rs";
const LINEAR_OWNERS: [(&str, &str); 5] = [
    ("IncrementalAlterConfigsHost", HOST_PATH),
    ("IncrementalAlterConfigsOperation", HOST_PATH),
    ("IncrementalAlterConfigsSubmission", HOST_PATH),
    (
        "IncrementalAlterConfigsShardOwner",
        "crates/kafka-client-engine/src/admin/alter_configs/shard.rs",
    ),
    (
        "IncrementalAlterConfigsObserver",
        "crates/kafka-client-engine/src/admin/alter_configs/observer.rs",
    ),
];

#[test]
fn incremental_engine_owners_are_registered_at_their_exact_modules() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR_OWNERS {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} must have one owner");
        assert_eq!(rules[0].path, path);
    }

    assert_mutation_rule(
        &config.mutation_owners,
        "IncrementalAlterConfigsHost",
        "retained_bytes",
        &[HOST_PATH, ADMISSION_PATH, TERMINAL_PATH],
    );
    assert_mutation_rule(
        &config.mutation_owners,
        "IncrementalAlterConfigsOperation",
        "handoff",
        &[HOST_PATH],
    );
}

#[test]
fn fixture_rejects_retained_byte_and_handoff_mutation_outside_each_owner() {
    let (root, files) = fixture_files("incremental_alter_configs_engine_ownership");
    let rules = [
        MutationOwner {
            owner_type: "IncrementalAlterConfigsHost".into(),
            field: "retained_bytes".into(),
            allowed_paths: vec!["src/host_mutation_owner.rs".into()],
        },
        MutationOwner {
            owner_type: "IncrementalAlterConfigsOperation".into(),
            field: "handoff".into(),
            allowed_paths: vec!["src/handoff_mutation_owner.rs".into()],
        },
    ];
    let violations = mutation_violations(&root, &files, &rules);
    for (file, owner_type, field) in [
        (
            "host_mutation_intruder.rs",
            "IncrementalAlterConfigsHost",
            "retained_bytes",
        ),
        (
            "handoff_mutation_intruder.rs",
            "IncrementalAlterConfigsOperation",
            "handoff",
        ),
    ] {
        assert!(violations.iter().any(|violation| {
            violation.contains(file) && violation.contains(owner_type) && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_clone_and_copy_for_every_linear_engine_owner() {
    let (root, files) = fixture_files("incremental_alter_configs_engine_ownership");
    let rules = LINEAR_OWNERS.map(|(owner_type, _path)| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &rules);
    for (owner_type, _path) in LINEAR_OWNERS {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}

fn assert_mutation_rule(
    rules: &[MutationOwner],
    owner_type: &str,
    field: &str,
    allowed_paths: &[&str],
) {
    let matches = rules
        .iter()
        .filter(|rule| rule.owner_type == owner_type && rule.field == field)
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "{owner_type}.{field} needs one owner");
    assert_eq!(
        matches[0].allowed_paths,
        allowed_paths
            .iter()
            .map(|path| (*path).to_owned())
            .collect::<Vec<_>>()
    );
}
