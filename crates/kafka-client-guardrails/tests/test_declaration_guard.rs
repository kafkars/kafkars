//! Every sibling `*_test.rs` unit test is declared and compiled.

mod support;

use std::path::{Path, PathBuf};

use support::{
    Declaration, declaration, display_path, fixture_files, is_unit_test, load_config, read,
    rust_files, sibling_facade, workspace_root,
};

fn undeclared_tests(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files.iter().filter(|path| is_unit_test(path)) {
        let relative = display_path(root, path);
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(facade) = sibling_facade(path) else {
            violations.push(format!(
                "{relative} has no sibling facade and runs zero tests"
            ));
            continue;
        };
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        match declaration(&read(&facade), stem, file_name) {
            Declaration::Gated => {}
            Declaration::Ungated => violations.push(format!(
                "{} declares `{stem}` without #[cfg(test)]",
                display_path(root, &facade)
            )),
            Declaration::Redirected => violations.push(format!(
                "{} redirects `{stem}` away from sibling test {relative}",
                display_path(root, &facade)
            )),
            Declaration::Absent => {
                violations.push(format!("{relative} is undeclared and runs zero tests"));
            }
        }
    }
    violations
}

#[test]
fn every_live_sibling_unit_test_is_declared_and_gated() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = undeclared_tests(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "unit-test declaration violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn undeclared_and_ungated_tests_are_rejected() {
    let (root, files) = fixture_files("undeclared_unit_test");
    let violations = undeclared_tests(&root, &files);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("orphan_test.rs"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("without #[cfg(test)]"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("redirects `redirected_test`"))
    );
}
