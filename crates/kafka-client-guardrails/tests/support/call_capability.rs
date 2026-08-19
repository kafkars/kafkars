//! Complete production allowlists for load-bearing constructor calls.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use super::invocation::InvocationEvidence;
use super::{
    CallCapabilityRule, WalkScope, display_path, invocations, is_test_only_source, read,
    rust_files_under,
};

pub(crate) fn call_capability_violations(root: &Path, rules: &[CallCapabilityRule]) -> Vec<String> {
    let mut violations = Vec::new();
    let mut file_cache = BTreeMap::<PathBuf, Vec<PathBuf>>::new();
    let mut evidence_cache = BTreeMap::<PathBuf, InvocationEvidence>::new();
    for rule in rules {
        let source_root = root.join(&rule.root);
        assert!(
            source_root.is_dir(),
            "call capability root {} is missing",
            source_root.display()
        );
        for allowed in &rule.allowed_paths {
            let path = root.join(allowed);
            if !path.is_file() || !path.starts_with(&source_root) {
                violations.push(format!(
                    "stale call capability path for {}: {allowed}",
                    rule.call
                ));
            }
        }
        let mut observed_allowed = BTreeSet::new();
        let files = file_cache.entry(source_root.clone()).or_insert_with(|| {
            rust_files_under(&source_root, WalkScope::Fixture)
                .into_iter()
                .filter(|path| !is_test_only_source(path))
                .collect()
        });
        for path in files {
            let observed = evidence_cache
                .entry(path.clone())
                .or_insert_with(|| collect_invocations(root, path));
            let relative = display_path(root, path);
            let protected_method = rule.call.rsplit("::").next().unwrap_or(&rule.call);
            if observed.macro_identifiers.contains(protected_method) {
                violations.push(format!(
                    "{relative} contains protected call token {protected_method} inside a macro"
                ));
                continue;
            }
            if !observed.invokes_candidate(&rule.call) {
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
                    "{relative} invokes call capability {} outside its complete allowlist",
                    rule.call
                ));
            }
        }
        for allowed in &rule.allowed_paths {
            if root.join(allowed).is_file() && !observed_allowed.contains(allowed) {
                violations.push(format!(
                    "decorative call capability path for {}: {allowed}",
                    rule.call
                ));
            }
        }
    }
    violations
}

fn collect_invocations(root: &Path, path: &Path) -> InvocationEvidence {
    let source = read(path);
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, path)));
    invocations(&syntax)
}
