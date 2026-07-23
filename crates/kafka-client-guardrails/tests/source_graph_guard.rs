//! Every Rust source is reachable from a bounded Cargo target and module graph.

mod support;

use support::{
    fixture_files, load_config, rust_files, workspace_root, workspace_source_violations,
};

#[test]
fn live_rust_sources_are_reachable_and_bounded() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let files = rust_files(&workspace, &config);
    let violations = workspace_source_violations(&workspace, &files);

    assert!(
        violations.is_empty(),
        "Rust source-graph violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn escaped_generated_and_unreachable_sources_are_rejected() {
    let (root, files) = fixture_files("source_graph_bypass");
    let violations = workspace_source_violations(&root, &files);

    for expected in [
        "uses unbounded #[path",
        "uses conditional #[path]",
        "uses include!",
        "indirect_include.rs uses include!",
        "macro_module.rs uses opaque macro expansion",
        "macro_generated.rs uses include!",
        "macro_scope_launder.rs uses opaque macro expansion",
        "shadowed_builtin.rs uses opaque macro expansion",
        "shadowed_core_extern.rs uses opaque macro expansion",
        "shadowed_import.rs uses opaque macro expansion",
        "shadowed_raw_root.rs uses opaque macro expansion",
        "shadowed_syn.rs uses opaque macro expansion",
        "orphan.rs is unreachable",
        "orphan/worker_test.rs is unreachable",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(expected)),
            "source graph missed `{expected}`: {violations:?}"
        );
    }
    assert!(
        violations
            .iter()
            .all(|value| !value.contains("target_specific.rs is unreachable")),
        "target-gated source escaped static reachability: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .all(|value| !value.contains("safe_macros.rs")),
        "ordinary expression macros were rejected: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .all(|value| !value.contains("trusted_qualified.rs")),
        "genuine std/core/syn macro roots were rejected: {violations:?}"
    );
}

#[test]
fn cargo_target_roots_cannot_escape_their_package() {
    let (root, files) = fixture_files("external_cargo_target");
    let violations = workspace_source_violations(&root, &files);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("Cargo target") && value.contains("bounded canonical")),
        "target escape was accepted: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("outside every workspace package")),
        "external target escaped inspection ownership: {violations:?}"
    );
}

#[test]
fn workspace_package_roots_cannot_overlap() {
    let (root, files) = fixture_files("overlapping_package_roots");
    let violations = workspace_source_violations(&root, &files);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("workspace package roots") && value.contains("overlap")),
        "overlapping package ownership was accepted: {violations:?}"
    );
}

#[test]
fn local_macro_proof_is_source_ordered_and_unconditional() {
    let (root, files) = fixture_files("macro_source_order");
    let parent = root.join("parent");
    let inspected = files
        .into_iter()
        .filter(|path| path.starts_with(&parent))
        .collect::<Vec<_>>();
    let violations = workspace_source_violations(&root, &inspected);

    for expected in [
        "conditional_decoy.rs uses opaque macro expansion",
        "later_decoy.rs uses opaque macro expansion",
        "parent/src/lib.rs uses opaque #[macro_use]",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(expected)),
            "macro scope accepted `{expected}`: {violations:?}"
        );
    }
    assert!(
        violations
            .iter()
            .all(|value| !value.contains("local_before.rs")),
        "definition-before-invocation proof was rejected: {violations:?}"
    );
}
