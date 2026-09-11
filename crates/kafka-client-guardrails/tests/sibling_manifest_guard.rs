//! Published Kafka crates retain exact registry requirements and locked artifacts.

#[path = "support/release_manifest.rs"]
mod release_manifest;
#[path = "support/sibling_manifest.rs"]
mod sibling_manifest;
mod support;

use std::path::{Path, PathBuf};

use release_manifest::release_dependency_violations;
use sibling_manifest::{lock_violations, violations};
use support::{read, workspace_root};

#[test]
fn live_publishable_dependencies_match_workspace_release_stability() {
    let workspace = workspace_root();
    let core = read(&workspace.join("crates/kafka-client-core/Cargo.toml"));
    let engine = read(&workspace.join("crates/kafka-client-engine/Cargo.toml"));
    let facade = read(&workspace.join("crates/kafkars/Cargo.toml"));
    let violations = release_dependency_violations(
        &read(&workspace.join("Cargo.toml")),
        &[
            ("kafka-client-core", &core),
            ("kafka-client-engine", &engine),
            ("kafkars", &facade),
        ],
    );

    assert!(
        violations.is_empty(),
        "release dependency violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn stable_workspace_rejects_prerelease_runtime_and_build_dependencies() {
    let root = "[workspace.package]\nversion = \"1.0.0\"\n\
                [workspace.dependencies]\npreview = \"=2.0.0-rc.1\"\n";
    let package = "[package]\nname = \"public\"\nversion = \"1.0.0\"\n\
                   [dependencies]\npreview.workspace = true\n\
                   [build-dependencies]\nbuilder = \"^3.0.0-beta.2\"\n";
    let violations = release_dependency_violations(root, &[("public", package)]);

    assert_eq!(violations.len(), 2, "unexpected violations: {violations:?}");
    assert!(violations.iter().any(|value| value.contains("preview")));
    assert!(violations.iter().any(|value| value.contains("builder")));
}

#[test]
fn prerelease_workspace_may_use_prerelease_dependencies() {
    let root = "[workspace.package]\nversion = \"1.0.0-rc.1\"\n";
    let package = "[package]\nname = \"public\"\nversion = \"1.0.0-rc.1\"\n\
                   [dependencies]\npreview = \"=2.0.0-rc.1\"\n";

    assert!(release_dependency_violations(root, &[("public", package)]).is_empty());
}

#[test]
fn stable_workspace_accepts_stable_dependency_requirements() {
    let root = "[workspace.package]\nversion = \"1.0.0\"\n\
                [workspace.dependencies]\nstable = \"1.2\"\n";
    let package = "[package]\nname = \"public\"\nversion = \"1.0.0\"\n\
                   publish = [\"crates-io\"]\n[dependencies]\nstable.workspace = true\n";

    assert!(release_dependency_violations(root, &[("public", package)]).is_empty());
}

#[test]
fn live_published_dependency_specs_have_exact_registry_versions() {
    let workspace = workspace_root();
    let violations = violations(
        &read(&workspace.join("Cargo.toml")),
        &read(&workspace.join("crates/kafka-client-engine/Cargo.toml")),
    );

    assert!(
        violations.is_empty(),
        "published dependency manifest violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn live_lock_binds_exact_published_artifacts() {
    let source = read(&workspace_root().join("Cargo.lock"));
    let violations = lock_violations(&source);

    assert!(
        violations.is_empty(),
        "published dependency lock violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn alternate_sources_checksums_and_duplicate_versions_are_rejected() {
    let source = read(&workspace_root().join("Cargo.lock"));
    let alternate_source = source.replace(
        "registry+https://github.com/rust-lang/crates.io-index",
        "git+https://example.invalid/kafka-driver",
    );
    assert!(!lock_violations(&alternate_source).is_empty());

    let alternate_checksum = source.replace(
        "d690cdc62d40d1a7ab7c6e796372277f85107e6e3a9819d58db99f48c5e9d044",
        "0000000000000000000000000000000000000000000000000000000000000000",
    );
    assert!(
        lock_violations(&alternate_checksum)
            .iter()
            .any(|violation| {
                violation
                    == "lockfile must bind kafka-driver 0.1.0-rc.6 to its exact crates.io checksum"
            })
    );

    let duplicate_wire = format!(
        "{source}\n[[package]]\nname = \"kafka-wire\"\nversion = \"0.1.0-rc.2\"\n\
         source = \"registry+https://github.com/rust-lang/crates.io-index\"\n\
         checksum = \"0000000000000000000000000000000000000000000000000000000000000000\"\n"
    );
    assert!(lock_violations(&duplicate_wire).iter().any(|violation| {
        violation == "lockfile must bind kafka-wire 0.1.0-rc.3 to its exact crates.io checksum"
    }));
}

#[test]
fn git_path_and_alternate_registry_specs_are_rejected() {
    let engine = fixture("engine_valid.toml");
    for root in [
        "root_git.toml",
        "root_branch.toml",
        "root_rev.toml",
        "root_tag.toml",
        "root_alternate_path.toml",
        "root_path_only.toml",
        "root_registry.toml",
    ] {
        let violations = violations(&fixture(root), &engine);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("workspace dependency kafka-driver")),
            "published dependency manifest guard accepted {root}: {violations:?}"
        );
    }
}

#[test]
fn wrong_registry_versions_are_rejected() {
    let engine = fixture("engine_valid.toml");
    let violations = violations(&fixture("root_wrong_version.toml"), &engine);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("workspace dependency kafka-driver")),
        "published dependency manifest guard accepted a wrong version: {violations:?}"
    );
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
            "published dependency manifest guard accepted {root}: {violations:?}"
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
        "published dependency manifest guard accepted a root target override: {violations:?}"
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
            "published dependency manifest guard accepted {engine}: {violations:?}"
        );
    }
}

#[test]
fn wire_core_is_exactly_normal_and_cannot_be_aliased_or_redeclared() {
    let root = fixture("root_valid.toml");
    let normal = violations(&root, &fixture("engine_wire_core_normal.toml"));
    assert!(
        normal.is_empty(),
        "published dependency manifest guard rejected exact normal wire-core: {normal:?}"
    );

    let development = violations(&root, &fixture("engine_wire_core_dev.toml"));
    assert!(
        development.iter().any(|violation| {
            violation
                .contains("engine dev-dependencies redeclares reviewed package kafka-wire-core")
        }),
        "published dependency manifest guard accepted dev-only wire-core: {development:?}"
    );

    let aliased = violations(&root, &fixture("engine_wire_core_alias.toml"));
    assert!(
        aliased
            .iter()
            .any(|violation| violation.contains("aliases reviewed package kafka-wire-core")),
        "published dependency manifest guard accepted aliased wire-core: {aliased:?}"
    );
}

fn fixture(name: &str) -> String {
    read(&fixture_root().join(name))
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sibling_manifest")
}
