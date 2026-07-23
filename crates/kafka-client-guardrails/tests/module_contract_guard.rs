//! Every Rust file states its responsibility before its implementation.

mod support;

use std::path::{Path, PathBuf};

use support::{display_path, fixture_files, load_config, read, rust_files, workspace_root};

fn files_without_contract(root: &Path, files: &[PathBuf]) -> Vec<String> {
    files
        .iter()
        .filter(|path| !has_module_contract(&read(path)))
        .map(|path| display_path(root, path))
        .collect()
}

fn has_module_contract(source: &str) -> bool {
    let source = source.trim_start();
    let mut lines = source.lines();
    let Some(first) = lines.next() else {
        return false;
    };
    if !first.starts_with("//!") {
        return false;
    }
    first
        .strip_prefix("//!")
        .is_some_and(|contract| !contract.trim().is_empty())
}

#[test]
fn every_live_rust_file_begins_with_a_module_contract() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = files_without_contract(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "Rust files without a leading `//!` module contract:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_missing_module_contract_is_rejected() {
    let (root, files) = fixture_files("module_without_contract");
    let violations = files_without_contract(&root, &files);

    assert_eq!(violations, ["src/bad.rs", "src/empty.rs"]);
}
