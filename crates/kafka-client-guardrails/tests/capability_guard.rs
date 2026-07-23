//! Source capabilities remain with the layer that owns their effects.

mod support;

use std::path::Path;

use support::{
    CapabilityRule, WalkScope, display_path, fixture_files, load_config, read, rust_files_under,
    workspace_root,
};
use syn::visit::Visit;
use syn::{ItemUse, UseTree};

fn capability_violations(root: &Path, rules: &[CapabilityRule]) -> Vec<String> {
    let mut violations = Vec::new();
    for rule in rules {
        let source_root = root.join(&rule.root);
        assert!(
            source_root.is_dir(),
            "capability root {} is missing",
            source_root.display()
        );
        for path in rust_files_under(&source_root, WalkScope::Fixture) {
            inspect_file(root, &path, rule, &mut violations);
        }
    }
    violations
}

fn inspect_file(root: &Path, path: &Path, rule: &CapabilityRule, violations: &mut Vec<String>) {
    let source = read(path);
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, path)));
    let mut collector = CapabilityCollector::default();
    collector.visit_file(&syntax);

    for observed in collector.paths {
        for forbidden in &rule.forbidden {
            if observed == *forbidden
                || observed
                    .strip_prefix(forbidden)
                    .is_some_and(|suffix| suffix.starts_with("::"))
            {
                violations.push(format!(
                    "{} reaches forbidden capability {forbidden} through {observed}",
                    display_path(root, path)
                ));
            }
        }
    }
}

#[derive(Default)]
struct CapabilityCollector {
    paths: Vec<String>,
}

impl<'ast> Visit<'ast> for CapabilityCollector {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        self.paths.push(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::"),
        );
        syn::visit::visit_path(self, path);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        collect_use_paths(String::new(), &item.tree, &mut self.paths);
        syn::visit::visit_item_use(self, item);
    }
}

fn collect_use_paths(prefix: String, tree: &UseTree, paths: &mut Vec<String>) {
    match tree {
        UseTree::Path(path) => {
            let next = append_segment(&prefix, &path.ident.to_string());
            collect_use_paths(next, &path.tree, paths);
        }
        UseTree::Name(name) => paths.push(append_segment(&prefix, &name.ident.to_string())),
        UseTree::Rename(rename) => {
            paths.push(append_segment(&prefix, &rename.ident.to_string()));
        }
        UseTree::Glob(_) => paths.push(prefix),
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_paths(prefix.clone(), item, paths);
            }
        }
    }
}

fn append_segment(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_owned()
    } else {
        format!("{prefix}::{segment}")
    }
}

#[test]
fn live_source_respects_capability_ownership() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = capability_violations(&workspace, &config.capability_rules);

    assert!(
        violations.is_empty(),
        "capability ownership violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_forbidden_socket_import_alias_is_rejected() {
    let (root, _) = fixture_files("forbidden_capability");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec!["std::net".to_owned()],
    }];
    let violations = capability_violations(&root, &rules);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("alias.rs") && value.contains("std::net")),
        "capability detector accepted an aliased socket import: {violations:?}"
    );
}
