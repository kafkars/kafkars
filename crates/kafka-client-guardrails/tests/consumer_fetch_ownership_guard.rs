//! Linear ownership registration and negative evidence for retained Fetch data.

mod support;

use support::{
    LinearOwner, fixture_files, linear_violations, load_config, rust_files, workspace_root,
};

const RETAINED_TYPES: [(&str, &str); 12] = [
    ("FetchResponse", "model.rs"),
    ("FetchTopic", "model.rs"),
    ("FetchPartition", "model.rs"),
    ("FetchEndpoint", "model.rs"),
    ("FetchBatch", "model.rs"),
    ("FetchRecord", "model.rs"),
    ("FetchHeader", "model.rs"),
    ("FetchOutcome", "outcome.rs"),
    ("RetainedFetchOutcome", "outcome.rs"),
    ("RejectedFetchOutcome", "outcome.rs"),
    ("FetchOutputReservation", "retention.rs"),
    ("FetchRetainedCharge", "retention.rs"),
];
const FETCH_PATH: &str = "crates/kafka-client-engine/src/protocol/fetch";

#[test]
fn retained_fetch_graph_is_registered_and_linear() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);

    for (owner_type, file) in RETAINED_TYPES {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, format!("{FETCH_PATH}/{file}"));
    }
    let violations = linear_violations(&workspace, &files, &config.linear_owners);
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn retained_fetch_fixture_rejects_clone_and_copy() {
    let (root, files) = fixture_files("consumer_fetch_ownership");
    let rules = RETAINED_TYPES.map(|(owner_type, _)| LinearOwner {
        owner_type: owner_type.into(),
        path: "src/linear_intruder.rs".into(),
    });
    let violations = linear_violations(&root, &files, &rules);

    for (owner_type, _) in RETAINED_TYPES {
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
