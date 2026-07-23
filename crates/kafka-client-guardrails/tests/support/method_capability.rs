//! Complete production allowlists for inferred method capabilities.

use std::{collections::BTreeSet, path::Path};

use super::{
    MethodCapabilityRule, WalkScope, display_path, invocation_matches, invocations,
    is_test_only_source, read, rust_files_under,
};

pub(crate) fn method_capability_violations(
    root: &Path,
    rules: &[MethodCapabilityRule],
) -> Vec<String> {
    let mut violations = Vec::new();
    for rule in rules {
        let source_root = root.join(&rule.root);
        assert!(
            source_root.is_dir(),
            "method capability root {} is missing",
            source_root.display()
        );
        validate_allowed_paths(root, &source_root, rule, &mut violations);
        inspect_rule(root, &source_root, rule, &mut violations);
    }
    violations
}

fn inspect_rule(
    root: &Path,
    source_root: &Path,
    rule: &MethodCapabilityRule,
    violations: &mut Vec<String>,
) {
    let mut observed_allowed = BTreeSet::new();
    for path in rust_files_under(source_root, WalkScope::Fixture)
        .into_iter()
        .filter(|path| !is_test_only_source(path))
    {
        let source = read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, &path)));
        let observed = invocations(&syntax);
        let relative = display_path(root, &path);
        if observed.macro_identifiers.contains(&rule.method) {
            violations.push(format!(
                "{relative} contains protected method token {} inside a macro",
                rule.method
            ));
            continue;
        }
        if !observed
            .paths
            .iter()
            .chain(&observed.unresolved)
            .any(|call| invocation_matches(call, &rule.method))
        {
            continue;
        }
        if rule
            .allowed_paths
            .iter()
            .any(|allowed| allowed == &relative)
        {
            observed_allowed.insert(relative);
        } else {
            violations.push(format!(
                "{relative} invokes method capability {} outside its complete allowlist",
                rule.method
            ));
        }
    }
    for allowed in &rule.allowed_paths {
        if root.join(allowed).is_file() && !observed_allowed.contains(allowed) {
            violations.push(format!(
                "decorative method capability path for {}: {allowed}",
                rule.method
            ));
        }
    }
}

fn validate_allowed_paths(
    root: &Path,
    source_root: &Path,
    rule: &MethodCapabilityRule,
    violations: &mut Vec<String>,
) {
    for allowed in &rule.allowed_paths {
        let path = root.join(allowed);
        if !path.is_file() || !path.starts_with(source_root) {
            violations.push(format!(
                "stale method capability path for {}: {allowed}",
                rule.method
            ));
        }
    }
}
