//! Stable repository traversal and Rust file-role classification.

use std::fs;
use std::path::{Path, PathBuf};

use super::{Declaration, GuardConfig, declaration, is_unit_test, sibling_facade};

/// Size-policy role for one Rust source file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileClass {
    Facade,
    Implementation,
    Test,
    Auxiliary,
}

/// Whether an inspection targets the live workspace or a deliberately tiny fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalkScope {
    Workspace,
    Fixture,
}

const WORKSPACE_RUST_FILE_FLOOR: usize = 20;

pub(crate) fn workspace_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map_or_else(|| manifest_dir.to_path_buf(), Path::to_path_buf)
}

pub(crate) fn rust_files(workspace: &Path, config: &GuardConfig) -> Vec<PathBuf> {
    let excluded = config
        .paths
        .excluded_roots
        .iter()
        .map(|root| workspace.join(root))
        .collect::<Vec<_>>();
    collect_roots(
        workspace,
        &config.paths.rust_roots,
        &excluded,
        WalkScope::Workspace,
    )
}

pub(crate) fn rust_files_under(root: &Path, scope: WalkScope) -> Vec<PathBuf> {
    collect_roots(root, &[String::new()], &[], scope)
}

pub(crate) fn fixture_files(name: &str) -> (PathBuf, Vec<PathBuf>) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    let files = rust_files_under(&root, WalkScope::Fixture);
    (root, files)
}

fn collect_roots(
    base: &Path,
    roots: &[String],
    excluded: &[PathBuf],
    scope: WalkScope,
) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        let path = base.join(root);
        assert!(
            path.is_dir(),
            "configured Rust root {} is missing",
            path.display()
        );
        collect(&path, excluded, &mut files);
    }
    files.sort();
    files.dedup();
    if scope == WalkScope::Workspace {
        assert!(
            files.len() >= WORKSPACE_RUST_FILE_FLOOR,
            "workspace traversal found only {} Rust files; expected at least {WORKSPACE_RUST_FILE_FLOOR}",
            files.len()
        );
    }
    files
}

fn collect(root: &Path, excluded: &[PathBuf], files: &mut Vec<PathBuf>) {
    if excluded.iter().any(|path| root == path) {
        return;
    }
    let entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("read directory {}: {error}", root.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", root.display()))
            .path();
        if path.is_dir() {
            collect(&path, excluded, files);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

pub(crate) fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

pub(crate) fn is_facade(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|value| value.to_str()),
        Some("lib.rs" | "mod.rs")
    )
}

pub(crate) fn is_test_only_source(path: &Path) -> bool {
    if is_unit_test(path) {
        return true;
    }
    let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(facade) = sibling_facade(path) else {
        return false;
    };
    declaration(&read(&facade), stem, file_name) == Declaration::Gated
}

pub(crate) fn classify(root: &Path, path: &Path) -> FileClass {
    let relative = display_path(root, path);
    if relative.contains("/tests/") || relative.ends_with("_test.rs") {
        FileClass::Test
    } else if relative.contains("/examples/")
        || relative.ends_with("/src/main.rs")
        || relative.contains("/src/bin/")
    {
        FileClass::Auxiliary
    } else if is_facade(path) {
        FileClass::Facade
    } else {
        FileClass::Implementation
    }
}
