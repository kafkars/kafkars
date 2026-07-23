//! Repository traversal cannot silently inspect an empty or partial tree.

mod support;

use support::{display_path, fixture_files, load_config, rust_files, workspace_root};

#[test]
fn live_walk_covers_the_workspace_and_excludes_negative_fixtures() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);

    assert!(
        files
            .iter()
            .any(|path| display_path(&workspace, path).ends_with("kafka-client-core/src/lib.rs")),
        "live traversal missed the deterministic core"
    );
    assert!(
        files
            .iter()
            .all(|path| !display_path(&workspace, path).contains("/tests/fixtures/")),
        "live traversal included deliberately invalid fixtures"
    );
}

#[test]
fn fixture_walk_still_observes_deliberately_invalid_source() {
    let (_, files) = fixture_files("module_without_contract");
    assert_eq!(
        files.len(),
        3,
        "fixture traversal should see every contract probe"
    );
}
