//! Cargo package and target-root discovery without executing package build logic.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use toml::Value;

use super::valid_relative_policy_path;

#[derive(Debug)]
pub(crate) struct PackageTargets {
    pub(crate) package_root: PathBuf,
    pub(crate) target_roots: Vec<PathBuf>,
}

pub(crate) fn workspace_targets(workspace: &Path) -> (Vec<PackageTargets>, Vec<String>) {
    let manifest = workspace.join("Cargo.toml");
    let Ok(document) = parse_manifest(&manifest) else {
        return (
            Vec::new(),
            vec![format!(
                "workspace manifest {} does not parse",
                manifest.display()
            )],
        );
    };
    let Some(members) = document
        .get("workspace")
        .and_then(Value::as_table)
        .and_then(|workspace| workspace.get("members"))
        .and_then(Value::as_array)
    else {
        return (
            Vec::new(),
            vec!["workspace manifest has no explicit members".to_owned()],
        );
    };
    let mut packages = Vec::new();
    let mut violations = Vec::new();
    for member in members {
        let Some(member) = member.as_str() else {
            violations.push("workspace member is not a string".to_owned());
            continue;
        };
        if !valid_relative_policy_path(member) {
            violations.push(format!(
                "workspace member `{member}` is not a canonical relative path"
            ));
            continue;
        }
        let package_root = workspace.join(member);
        if !canonically_within(workspace, &package_root) {
            violations.push(format!(
                "workspace member `{member}` escapes the workspace after canonicalization"
            ));
            continue;
        }
        let (targets, mut target_violations) = package_targets(&package_root);
        violations.append(&mut target_violations);
        packages.push(PackageTargets {
            package_root,
            target_roots: targets,
        });
    }
    for (index, package) in packages.iter().enumerate() {
        for other in packages.iter().skip(index + 1) {
            if roots_overlap(&package.package_root, &other.package_root) {
                violations.push(format!(
                    "workspace package roots {} and {} overlap",
                    package.package_root.display(),
                    other.package_root.display()
                ));
            }
        }
    }
    (packages, violations)
}

fn roots_overlap(left: &Path, right: &Path) -> bool {
    matches!(
        (left.canonicalize(), right.canonicalize()),
        (Ok(left), Ok(right)) if left.starts_with(&right) || right.starts_with(&left)
    )
}

pub(crate) fn package_targets(package_root: &Path) -> (Vec<PathBuf>, Vec<String>) {
    let manifest = package_root.join("Cargo.toml");
    let Ok(document) = parse_manifest(&manifest) else {
        return (
            Vec::new(),
            vec![format!(
                "package manifest {} does not parse",
                manifest.display()
            )],
        );
    };
    let Some(package) = document.get("package").and_then(Value::as_table) else {
        return (
            Vec::new(),
            vec![format!("{} has no package table", manifest.display())],
        );
    };
    let mut candidates = Vec::new();
    let mut violations = Vec::new();
    explicit_or_default_lib(package_root, &document, package, &mut candidates);
    explicit_targets(
        package_root,
        &document,
        "bin",
        &mut candidates,
        &mut violations,
    );
    explicit_targets(
        package_root,
        &document,
        "test",
        &mut candidates,
        &mut violations,
    );
    explicit_targets(
        package_root,
        &document,
        "example",
        &mut candidates,
        &mut violations,
    );
    explicit_targets(
        package_root,
        &document,
        "bench",
        &mut candidates,
        &mut violations,
    );
    if auto_enabled(package, "autobins") {
        push_if_file(&mut candidates, package_root.join("src/main.rs"));
        collect_auto_targets(&package_root.join("src/bin"), &mut candidates);
    }
    if auto_enabled(package, "autotests") {
        collect_auto_targets(&package_root.join("tests"), &mut candidates);
    }
    if auto_enabled(package, "autoexamples") {
        collect_auto_targets(&package_root.join("examples"), &mut candidates);
    }
    if auto_enabled(package, "autobenches") {
        collect_auto_targets(&package_root.join("benches"), &mut candidates);
    }
    collect_build_script(package_root, package, &mut candidates, &mut violations);
    let mut roots = BTreeSet::new();
    for candidate in candidates {
        match bounded_target(package_root, &candidate) {
            Ok(path) => {
                roots.insert(path);
            }
            Err(error) => violations.push(error),
        }
    }
    (roots.into_iter().collect(), violations)
}

fn parse_manifest(path: &Path) -> Result<Value, ()> {
    fs::read_to_string(path)
        .map_err(|_| ())
        .and_then(|source| source.parse::<Value>().map_err(|_| ()))
}

fn explicit_or_default_lib(
    package_root: &Path,
    document: &Value,
    package: &toml::map::Map<String, Value>,
    candidates: &mut Vec<PathBuf>,
) {
    if let Some(path) = document
        .get("lib")
        .and_then(Value::as_table)
        .and_then(|target| target.get("path"))
        .and_then(Value::as_str)
    {
        candidates.push(package_root.join(path));
    } else if auto_enabled(package, "autolib") {
        push_if_file(candidates, package_root.join("src/lib.rs"));
    }
}

fn explicit_targets(
    package_root: &Path,
    document: &Value,
    kind: &str,
    candidates: &mut Vec<PathBuf>,
    violations: &mut Vec<String>,
) {
    let Some(targets) = document.get(kind).and_then(Value::as_array) else {
        return;
    };
    for target in targets {
        match target
            .as_table()
            .and_then(|target| target.get("path"))
            .and_then(Value::as_str)
        {
            Some(path) => candidates.push(package_root.join(path)),
            None => violations.push(format!(
                "{} has an explicit [[{kind}]] without a path",
                package_root.display()
            )),
        }
    }
}

fn collect_build_script(
    package_root: &Path,
    package: &toml::map::Map<String, Value>,
    candidates: &mut Vec<PathBuf>,
    violations: &mut Vec<String>,
) {
    match package.get("build") {
        Some(Value::String(path)) => candidates.push(package_root.join(path)),
        Some(Value::Boolean(false)) => {}
        Some(_) => violations.push(format!(
            "{} has a non-string package.build",
            package_root.display()
        )),
        None => push_if_file(candidates, package_root.join("build.rs")),
    }
}

fn auto_enabled(package: &toml::map::Map<String, Value>, key: &str) -> bool {
    package.get(key).and_then(Value::as_bool).unwrap_or(true)
}

fn collect_auto_targets(directory: &Path, candidates: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            candidates.push(path);
        } else if path.is_dir() {
            push_if_file(candidates, path.join("main.rs"));
        }
    }
}

fn push_if_file(candidates: &mut Vec<PathBuf>, path: PathBuf) {
    if path.is_file() {
        candidates.push(path);
    }
}

fn bounded_target(package_root: &Path, candidate: &Path) -> Result<PathBuf, String> {
    let relative = candidate.strip_prefix(package_root).map_err(|_| {
        format!(
            "Cargo target {} escapes package {}",
            candidate.display(),
            package_root.display()
        )
    })?;
    let relative = relative.to_string_lossy().replace('\\', "/");
    if !valid_relative_policy_path(&relative) {
        return Err(format!(
            "Cargo target {} is not a bounded canonical path",
            candidate.display()
        ));
    }
    if !candidate.is_file() {
        return Err(format!("Cargo target {} is missing", candidate.display()));
    }
    if !canonically_within(package_root, candidate) {
        return Err(format!(
            "Cargo target {} escapes package {} after canonicalization",
            candidate.display(),
            package_root.display()
        ));
    }
    Ok(candidate.to_path_buf())
}

fn canonically_within(root: &Path, candidate: &Path) -> bool {
    matches!(
        (root.canonicalize(), candidate.canonicalize()),
        (Ok(root), Ok(candidate)) if candidate.starts_with(&root)
    )
}
