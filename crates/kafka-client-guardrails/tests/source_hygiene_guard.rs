//! Production reading paths exclude embedded tests and unfinished escape hatches.

mod support;

use std::path::{Path, PathBuf};

use support::{display_path, fixture_files, load_config, read, rust_files, workspace_root};

fn source_hygiene_violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files {
        let relative = display_path(root, path);
        if relative.contains("/tests/") || relative.ends_with("_test.rs") {
            continue;
        }
        let source = read(path);
        for forbidden in ["todo!", "unimplemented!", "dbg!"] {
            if source.contains(forbidden) {
                violations.push(format!("{relative} contains forbidden `{forbidden}`"));
            }
        }
        if source.lines().any(|line| line.trim() == "#[test]") {
            violations.push(format!(
                "{relative} embeds a test function; move it to a sibling `*_test.rs` file"
            ));
        }
        if embeds_test_body(&source) {
            violations.push(format!("{relative} embeds an inline test module"));
        }
    }
    violations
}

fn embeds_test_body(source: &str) -> bool {
    let lines = source.lines().collect::<Vec<_>>();
    for (index, line) in lines.iter().enumerate() {
        if line.trim() != "#[cfg(test)]" {
            continue;
        }
        let next = lines
            .iter()
            .skip(index + 1)
            .map(|value| value.trim())
            .find(|value| !value.is_empty());
        if next.is_some_and(|value| value.starts_with("mod ") && value.contains('{')) {
            return true;
        }
    }
    false
}

#[test]
fn production_sources_are_finished_and_keep_tests_separate() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = source_hygiene_violations(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "source hygiene violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn embedded_tests_and_placeholders_are_rejected() {
    let (root, files) = fixture_files("inline_test_body");
    let violations = source_hygiene_violations(&root, &files);

    assert!(violations.iter().any(|value| value.contains("inline test")));
    assert!(
        violations
            .iter()
            .any(|value| value.contains("test function"))
    );
    assert!(violations.iter().any(|value| value.contains("todo!")));
}
