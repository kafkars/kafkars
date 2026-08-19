//! Registration and negative evidence for offset-deletion engine ownership.

mod support;

use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const HOST: &str = "crates/kafka-client-engine/src/admin/group_offset_delete/host.rs";
const MODEL: &str = "crates/kafka-client-engine/src/admin/group_offset_delete/host/model.rs";
const ADMISSION: &str =
    "crates/kafka-client-engine/src/admin/group_offset_delete/host/admission.rs";
const TERMINAL: &str = "crates/kafka-client-engine/src/admin/group_offset_delete/host/terminal.rs";
const OWNERS: [(&str, &str); 6] = [
    ("DeleteConsumerGroupOffsetsHost", HOST),
    ("DeleteConsumerGroupOffsetsOperation", MODEL),
    ("DeleteConsumerGroupOffsetsSubmission", MODEL),
    (
        "DeleteConsumerGroupOffsetsShardOwner",
        "crates/kafka-client-engine/src/admin/group_offset_delete/shard.rs",
    ),
    (
        "DeleteConsumerGroupOffsetsObserver",
        "crates/kafka-client-engine/src/admin/group_offset_delete/observer.rs",
    ),
    (
        "DeleteConsumerGroupOffsetsAccepted",
        "crates/kafka-client-engine/src/admin/group_offset_delete/handle.rs",
    ),
];

#[test]
fn concrete_engine_owners_and_byte_mutations_are_registered_exactly() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in OWNERS {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} must have one owner");
        assert_eq!(rules[0].path, path);
    }
    let retained = config
        .mutation_owners
        .iter()
        .filter(|rule| {
            rule.owner_type == "DeleteConsumerGroupOffsetsHost" && rule.field == "retained_bytes"
        })
        .collect::<Vec<_>>();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        retained[0].allowed_paths,
        vec![HOST.to_owned(), ADMISSION.to_owned(), TERMINAL.to_owned()]
    );
}

#[test]
fn fixture_rejects_clone_copy_and_cross_owner_byte_mutation() {
    let (root, files) = fixture_files("delete_consumer_group_offsets_engine_ownership");
    let linear = OWNERS.map(|(owner_type, _path)| LinearOwner {
        owner_type: owner_type.to_owned(),
        path: "src/linear_intruder.rs".to_owned(),
    });
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _path) in OWNERS {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = [MutationOwner {
        owner_type: "DeleteConsumerGroupOffsetsHost".to_owned(),
        field: "retained_bytes".to_owned(),
        allowed_paths: vec!["src/byte_owner.rs".to_owned()],
    }];
    let violations = mutation_violations(&root, &files, &mutations);
    assert!(violations.iter().any(|violation| {
        violation.contains("byte_intruder.rs")
            && violation.contains("DeleteConsumerGroupOffsetsHost")
            && violation.contains("retained_bytes")
    }));
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("byte_owner.rs"))
    );
}
