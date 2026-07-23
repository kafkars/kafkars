//! Registered production modules retain declared sibling test evidence.

mod support;

use std::collections::BTreeSet;
use std::path::{Component, Path};

use support::{
    Declaration, TestMirror, declaration, display_path, fixture_files, is_unit_test, load_config,
    read, sibling_facade, workspace_root,
};

fn mirror_violations(root: &Path, mirrors: &[TestMirror]) -> Vec<String> {
    let mut productions = BTreeSet::new();
    let mut tests = BTreeSet::new();
    let mut violations = Vec::new();

    for mirror in mirrors {
        if !valid_policy_path(&mirror.production) || !valid_policy_path(&mirror.test) {
            violations.push(format!(
                "test mirror uses a non-canonical relative path: {} -> {}",
                mirror.production, mirror.test
            ));
            continue;
        }
        if !productions.insert(&mirror.production) {
            violations.push(format!(
                "production module {} has duplicate test-mirror owners",
                mirror.production
            ));
        }
        if !tests.insert(&mirror.test) {
            violations.push(format!(
                "test module {} has duplicate production owners",
                mirror.test
            ));
        }

        let production = root.join(&mirror.production);
        let test = root.join(&mirror.test);
        if !production.is_file() {
            violations.push(format!(
                "registered production module {} is missing",
                mirror.production
            ));
            continue;
        }
        if !test.is_file() {
            violations.push(format!("registered test module {} is missing", mirror.test));
            continue;
        }
        if !is_unit_test(&test) {
            violations.push(format!(
                "registered test module {} is not a `src/**/*_test.rs` file",
                mirror.test
            ));
            continue;
        }
        if production.parent() != test.parent() {
            violations.push(format!(
                "{} is not a sibling of production module {}",
                mirror.test, mirror.production
            ));
            continue;
        }

        let Some(stem) = test.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(file_name) = test.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(facade) = sibling_facade(&test) else {
            violations.push(format!(
                "{} has no nearest sibling facade",
                display_path(root, &test)
            ));
            continue;
        };
        match declaration(&read(&facade), stem, file_name) {
            Declaration::Gated => {}
            Declaration::Ungated => violations.push(format!(
                "{} declares registered test `{stem}` without #[cfg(test)]",
                display_path(root, &facade)
            )),
            Declaration::Redirected => violations.push(format!(
                "{} redirects registered test `{stem}`",
                display_path(root, &facade)
            )),
            Declaration::Absent => violations.push(format!(
                "{} does not declare registered test {}",
                display_path(root, &facade),
                mirror.test
            )),
        }
    }

    violations
}

fn valid_policy_path(value: &str) -> bool {
    if value.is_empty() || value.contains('\\') {
        return false;
    }
    let normalized = Path::new(value)
        .components()
        .map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .map(|components| components.join("/"));
    normalized.as_deref() == Some(value)
}

#[test]
fn live_registered_test_mirrors_are_sibling_declared_and_gated() {
    let workspace = workspace_root();
    let config = load_config(&workspace);

    assert!(
        !config.test_mirrors.is_empty(),
        "test-mirror registry must name load-bearing modules"
    );
    let violations = mirror_violations(&workspace, &config.test_mirrors);
    assert!(
        violations.is_empty(),
        "test-mirror policy violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_registered_sibling_test_layout_is_accepted() {
    let (root, _) = fixture_files("separated_test_layout");
    let mirrors = [TestMirror {
        production: "src/worker.rs".to_owned(),
        test: "src/worker_test.rs".to_owned(),
    }];

    assert!(mirror_violations(&root, &mirrors).is_empty());

    let (root, _) = fixture_files("split_facade_test_layout");
    let mirrors = [TestMirror {
        production: "src/store/materialization_view.rs".to_owned(),
        test: "src/store/materialization_view_test.rs".to_owned(),
    }];
    assert!(mirror_violations(&root, &mirrors).is_empty());
}

#[test]
fn malformed_or_stale_registered_mirrors_are_rejected() {
    let (root, _) = fixture_files("misplaced_test_layout");
    let mirrors = [
        TestMirror {
            production: "src/worker.rs".to_owned(),
            test: "src/nested/worker_test.rs".to_owned(),
        },
        TestMirror {
            production: "src/missing.rs".to_owned(),
            test: "src/nested/worker_test.rs".to_owned(),
        },
        TestMirror {
            production: "src/worker.rs".to_owned(),
            test: "src/missing_test.rs".to_owned(),
        },
        TestMirror {
            production: "../escaped.rs".to_owned(),
            test: "src/nested/worker_test.rs".to_owned(),
        },
        TestMirror {
            production: "src//worker.rs".to_owned(),
            test: "src/nested/worker_test.rs".to_owned(),
        },
    ];
    let violations = mirror_violations(&root, &mirrors);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("not a sibling"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("production module src/missing.rs is missing"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("test module src/missing_test.rs is missing"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("duplicate test-mirror owners"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("duplicate production owners"))
    );
    assert_eq!(
        violations
            .iter()
            .filter(|value| value.contains("non-canonical relative path"))
            .count(),
        2
    );
}
