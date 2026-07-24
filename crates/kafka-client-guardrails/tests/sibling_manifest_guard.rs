//! Reviewed sibling crates remain exact local paths throughout Cargo manifests.

#[path = "support/sibling_manifest.rs"]
mod sibling_manifest;
mod support;

use std::path::{Path, PathBuf};

use sibling_manifest::violations;
use support::{read, workspace_root};

#[test]
fn live_sibling_dependency_specs_are_exact_paths() {
    let workspace = workspace_root();
    let violations = violations(
        &read(&workspace.join("Cargo.toml")),
        &read(&workspace.join("crates/kafka-client-engine/Cargo.toml")),
    );

    assert!(
        violations.is_empty(),
        "sibling manifest violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn git_and_alternate_path_specs_are_rejected() {
    let engine = fixture("engine_valid.toml");
    for root in [
        "root_git.toml",
        "root_branch.toml",
        "root_rev.toml",
        "root_tag.toml",
        "root_registry.toml",
        "root_alternate_path.toml",
    ] {
        let violations = violations(&fixture(root), &engine);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("workspace dependency kafka-driver")),
            "sibling manifest guard accepted {root}: {violations:?}"
        );
    }
}

#[test]
fn aliases_and_cargo_source_overrides_are_rejected() {
    let engine = fixture("engine_valid.toml");
    for (root, expected) in [
        ("root_alias.toml", "aliases reviewed package kafka-driver"),
        ("root_patch.toml", "may not declare [patch]"),
        ("root_replace.toml", "may not declare [replace]"),
    ] {
        let violations = violations(&fixture(root), &engine);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "sibling manifest guard accepted {root}: {violations:?}"
        );
    }
}

#[test]
fn target_specific_root_specs_are_rejected() {
    let violations = violations(&fixture("root_target.toml"), &fixture("engine_valid.toml"));

    assert!(
        violations.iter().any(|violation| {
            violation.contains(
                "workspace root target cfg(unix) dependencies redeclares reviewed package \
                 kafka-driver",
            )
        }),
        "sibling manifest guard accepted a root target override: {violations:?}"
    );
}

#[test]
fn direct_aliased_target_development_and_build_engine_specs_are_rejected() {
    let root = fixture("root_valid.toml");
    for (engine, expected) in [
        (
            "engine_direct.toml",
            "engine dependency kafka-driver must be exactly",
        ),
        ("engine_alias.toml", "aliases reviewed package kafka-driver"),
        (
            "engine_target.toml",
            "target cfg(unix) dependencies redeclares reviewed package kafka-driver",
        ),
        (
            "engine_dev.toml",
            "engine dev-dependencies redeclares reviewed package kafka-driver",
        ),
        (
            "engine_build.toml",
            "engine build-dependencies redeclares reviewed package kafka-driver",
        ),
    ] {
        let violations = violations(&root, &fixture(engine));
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(expected)),
            "sibling manifest guard accepted {engine}: {violations:?}"
        );
    }
}

#[test]
fn wire_core_is_exactly_normal_and_cannot_be_aliased_or_redeclared() {
    let root = fixture("root_valid.toml");
    let normal = violations(&root, &fixture("engine_wire_core_normal.toml"));
    assert!(
        normal.is_empty(),
        "sibling manifest guard rejected exact normal wire-core: {normal:?}"
    );

    let development = violations(&root, &fixture("engine_wire_core_dev.toml"));
    assert!(
        development.iter().any(|violation| {
            violation
                .contains("engine dev-dependencies redeclares reviewed package kafka-wire-core")
        }),
        "sibling manifest guard accepted dev-only wire-core: {development:?}"
    );

    let aliased = violations(&root, &fixture("engine_wire_core_alias.toml"));
    assert!(
        aliased
            .iter()
            .any(|violation| violation.contains("aliases reviewed package kafka-wire-core")),
        "sibling manifest guard accepted aliased wire-core: {aliased:?}"
    );
}

fn fixture(name: &str) -> String {
    read(&fixture_root().join(name))
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sibling_manifest")
}
