//! Linear ownership registration and negative evidence for retained Fetch data.

mod support;

use support::{
    LinearOwner, fixture_files, linear_violations, load_config, rust_files, workspace_root,
};

const RETAINED_TYPES: [&str; 7] = [
    "FetchResponse",
    "FetchTopic",
    "FetchPartition",
    "FetchEndpoint",
    "FetchBatch",
    "FetchRecord",
    "FetchHeader",
];
const MODEL_PATH: &str = "crates/kafka-client-engine/src/protocol/fetch/model.rs";

#[test]
fn retained_fetch_graph_is_registered_and_linear() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);

    for owner_type in RETAINED_TYPES {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, MODEL_PATH);
    }
    let violations = linear_violations(&workspace, &files, &config.linear_owners);
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn retained_fetch_fixture_rejects_clone_and_copy() {
    let (root, files) = fixture_files("consumer_fetch_ownership");
    let rules = RETAINED_TYPES.map(|owner_type| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &rules);

    for owner_type in RETAINED_TYPES {
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
