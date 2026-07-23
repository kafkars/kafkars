//! Complete production allowlists for load-bearing constructor calls.

use std::{collections::BTreeSet, path::Path};

use super::{
    CallCapabilityRule, WalkScope, display_path, invocation_candidate_matches, invocations,
    is_test_only_source, read, rust_files_under,
};

pub(crate) fn call_capability_violations(root: &Path, rules: &[CallCapabilityRule]) -> Vec<String> {
    let mut violations = Vec::new();
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
        for path in rust_files_under(&source_root, WalkScope::Fixture)
            .into_iter()
            .filter(|path| !is_test_only_source(path))
        {
            let source = read(&path);
            let syntax = syn::parse_file(&source)
                .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, &path)));
            let observed = invocations(&syntax);
            let relative = display_path(root, &path);
            let protected_method = rule.call.rsplit("::").next().unwrap_or(&rule.call);
            if observed.macro_identifiers.contains(protected_method) {
                violations.push(format!(
                    "{relative} contains protected call token {protected_method} inside a macro"
                ));
                continue;
            }
            let candidates = observed.paths.iter().chain(&observed.unresolved);
            if !candidates
                .clone()
                .any(|call| invocation_candidate_matches(call, &rule.call))
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
