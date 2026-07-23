//! Workspace dependency edges are exact allowlists, not architectural suggestions.

#![allow(clippy::unwrap_used)]

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use support::{DependencyRule, load_config, read, workspace_root};

fn dependency_violations(crates_root: &Path, rules: &[DependencyRule]) -> Vec<String> {
    let manifests = crate_manifests(crates_root);
    let packages = manifests
        .iter()
        .map(|path| manifest_package(path).0)
        .collect::<BTreeSet<_>>();
    let rules = rules
        .iter()
        .map(|rule| (rule.package.as_str(), rule))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut violations = Vec::new();

    for manifest in manifests {
        let (package, dependencies) = manifest_package(&manifest);
        seen.insert(package.clone());
        let Some(rule) = rules.get(package.as_str()) else {
            violations.push(format!("{package} has no dependency rule"));
            continue;
        };
        let actual_internal = dependencies
            .iter()
            .filter(|name| packages.contains(*name))
            .cloned()
            .collect::<BTreeSet<_>>();
        let actual_external = dependencies
            .iter()
            .filter(|name| !packages.contains(*name))
            .cloned()
            .collect::<BTreeSet<_>>();
        compare_edges(
            &package,
            "internal",
            &actual_internal,
            &rule.allowed_internal,
            &mut violations,
        );
        compare_edges(
            &package,
            "external",
            &actual_external,
            &rule.allowed_external,
            &mut violations,
        );
    }
    for package in rules.keys() {
        if !seen.contains(*package) {
            violations.push(format!("dependency policy names missing package {package}"));
        }
    }
    violations
}

fn compare_edges(
    package: &str,
    kind: &str,
    actual: &BTreeSet<String>,
    allowed: &[String],
    violations: &mut Vec<String>,
) {
    let allowed = allowed.iter().cloned().collect::<BTreeSet<_>>();
    for dependency in actual.difference(&allowed) {
        violations.push(format!(
            "{package} has unreviewed {kind} dependency {dependency}"
        ));
    }
    for dependency in allowed.difference(actual) {
        violations.push(format!(
            "{package} has stale allowed {kind} dependency {dependency}"
        ));
    }
}

fn crate_manifests(crates_root: &Path) -> Vec<std::path::PathBuf> {
    let mut manifests = fs::read_dir(crates_root)
        .unwrap_or_else(|error| panic!("read {}: {error}", crates_root.display()))
        .filter_map(|entry| entry.ok().map(|value| value.path().join("Cargo.toml")))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    manifests.sort();
    manifests
}

fn manifest_package(path: &Path) -> (String, BTreeSet<String>) {
    let value = read(path)
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    let package = value["package"]["name"].as_str().unwrap().to_owned();
    let mut dependencies = BTreeSet::new();
    for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
        collect_dependencies(value.get(section), &mut dependencies);
    }
    if let Some(targets) = value.get("target").and_then(toml::Value::as_table) {
        for target in targets.values() {
            for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
                collect_dependencies(target.get(section), &mut dependencies);
            }
        }
    }
    (package, dependencies)
}

fn collect_dependencies(value: Option<&toml::Value>, dependencies: &mut BTreeSet<String>) {
    let Some(table) = value.and_then(toml::Value::as_table) else {
        return;
    };
    for (declared, specification) in table {
        let package = specification
            .get("package")
            .and_then(toml::Value::as_str)
            .unwrap_or(declared);
        dependencies.insert(package.to_owned());
    }
}

fn forbidden_lockfile_dependencies(lockfile: &Path, forbidden: &[String]) -> Vec<String> {
    let value = read(lockfile)
        .parse::<toml::Value>()
        .unwrap_or_else(|error| panic!("parse {}: {error}", lockfile.display()));
    let forbidden = forbidden
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let packages = value
        .get("package")
        .and_then(toml::Value::as_array)
        .unwrap_or_else(|| panic!("{} has no package array", lockfile.display()));
    let mut violations = packages
        .iter()
        .filter_map(|package| package.get("name").and_then(toml::Value::as_str))
        .filter(|name| forbidden.contains(name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    violations.sort();
    violations.dedup();
    violations
}

#[test]
fn live_dependency_graph_matches_the_reviewed_direction() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = dependency_violations(&workspace.join("crates"), &config.dependency_rules);

    assert!(
        violations.is_empty(),
        "dependency architecture violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn resolved_graph_contains_no_general_async_runtime() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = forbidden_lockfile_dependencies(
        &workspace.join("Cargo.lock"),
        &config.forbidden_transitive_dependencies,
    );

    assert!(
        violations.is_empty(),
        "resolved graph contains forbidden async runtime packages: {}",
        violations.join(", ")
    );
}

#[test]
fn a_core_dependency_on_the_engine_is_rejected() {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/reversed_dependency/crates");
    let rules = [
        DependencyRule {
            package: "fixture-core".to_owned(),
            allowed_internal: Vec::new(),
            allowed_external: Vec::new(),
        },
        DependencyRule {
            package: "fixture-engine".to_owned(),
            allowed_internal: Vec::new(),
            allowed_external: Vec::new(),
        },
    ];
    let violations = dependency_violations(&root, &rules);

    assert!(
        violations
            .iter()
            .any(|value| { value.contains("fixture-core") && value.contains("fixture-engine") }),
        "dependency detector accepted a reversed edge: {violations:?}"
    );
}

#[test]
fn a_transitive_async_runtime_in_the_lockfile_is_rejected() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/async_runtime_dependency/Cargo.lock");
    let forbidden = vec!["tokio".to_owned()];
    let violations = forbidden_lockfile_dependencies(&fixture, &forbidden);

    assert_eq!(violations, ["tokio"]);
}
