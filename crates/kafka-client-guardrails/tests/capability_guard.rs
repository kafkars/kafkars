//! Source capabilities remain with the layer that owns their effects.

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use support::{
    CapabilityRule, WalkScope, display_path, fixture_files, load_config, read, rust_files_under,
    workspace_root,
};
use syn::visit::Visit;
use syn::{Block, File, Item, ItemMod, ItemUse, Stmt, UseTree};

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

    for observed in &collector.paths {
        for forbidden in &rule.forbidden {
            if observed == forbidden
                || observed
                    .strip_prefix(forbidden)
                    .is_some_and(|suffix| suffix.starts_with("::"))
                || collector.globs.iter().any(|prefix| {
                    forbidden
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with("::"))
                })
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
    paths: BTreeSet<String>,
    globs: BTreeSet<String>,
    scopes: Vec<BTreeMap<String, String>>,
}

impl<'ast> Visit<'ast> for CapabilityCollector {
    fn visit_file(&mut self, file: &'ast File) {
        self.push_item_scope(&file.items);
        for item in &file.items {
            self.visit_item(item);
        }
        self.scopes.pop();
    }

    fn visit_item_mod(&mut self, module: &'ast ItemMod) {
        let Some((_, items)) = &module.content else {
            return;
        };
        self.push_item_scope(items);
        for item in items {
            self.visit_item(item);
        }
        self.scopes.pop();
    }

    fn visit_block(&mut self, block: &'ast Block) {
        self.scopes.push(block_aliases(block));
        for statement in &block.stmts {
            self.visit_stmt(statement);
        }
        self.scopes.pop();
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        let raw = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join("::");
        self.observe_path(&raw);
        syn::visit::visit_path(self, path);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        collect_use_paths(String::new(), &item.tree, &mut self.paths, &mut self.globs);
    }
}

impl CapabilityCollector {
    fn push_item_scope(&mut self, items: &[Item]) {
        let mut aliases = BTreeMap::new();
        for item in items {
            if let Item::Use(import) = item {
                collect_aliases("", &import.tree, &mut aliases);
            }
        }
        self.scopes.push(aliases);
    }

    fn observe_path(&mut self, path: &str) {
        self.paths.insert(path.to_owned());
        let (first, suffix) = path
            .split_once("::")
            .map_or((path, ""), |(first, suffix)| (first, suffix));
        if let Some(prefix) = self.scopes.iter().rev().find_map(|scope| scope.get(first)) {
            let resolved = if suffix.is_empty() {
                prefix.clone()
            } else {
                format!("{prefix}::{suffix}")
            };
            self.paths.insert(resolved);
        }
    }
}

fn collect_use_paths(
    prefix: String,
    tree: &UseTree,
    paths: &mut BTreeSet<String>,
    globs: &mut BTreeSet<String>,
) {
    match tree {
        UseTree::Path(path) => {
            let next = append_segment(&prefix, &path.ident.to_string());
            collect_use_paths(next, &path.tree, paths, globs);
        }
        UseTree::Name(name) => {
            let name = name.ident.to_string();
            let full = if name == "self" {
                prefix.clone()
            } else {
                append_segment(&prefix, &name)
            };
            paths.insert(full);
        }
        UseTree::Rename(rename) => {
            let full = append_segment(&prefix, &rename.ident.to_string());
            paths.insert(full);
        }
        UseTree::Glob(_) => {
            globs.insert(prefix);
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_paths(prefix.clone(), item, paths, globs);
            }
        }
    }
}

fn collect_aliases(prefix: &str, tree: &UseTree, aliases: &mut BTreeMap<String, String>) {
    match tree {
        UseTree::Path(path) => {
            let next = append_segment(prefix, &path.ident.to_string());
            collect_aliases(&next, &path.tree, aliases);
        }
        UseTree::Name(name) => {
            let name = name.ident.to_string();
            let full = if name == "self" {
                prefix.to_owned()
            } else {
                append_segment(prefix, &name)
            };
            let local = if name == "self" {
                prefix.rsplit("::").next().unwrap_or_default().to_owned()
            } else {
                name
            };
            aliases.insert(local, full);
        }
        UseTree::Rename(rename) => {
            aliases.insert(
                rename.rename.to_string(),
                append_segment(prefix, &rename.ident.to_string()),
            );
        }
        UseTree::Glob(_) => {}
        UseTree::Group(group) => {
            for item in &group.items {
                collect_aliases(prefix, item, aliases);
            }
        }
    }
}

fn block_aliases(block: &Block) -> BTreeMap<String, String> {
    let mut aliases = BTreeMap::new();
    for statement in &block.stmts {
        if let Stmt::Item(Item::Use(import)) = statement {
            collect_aliases("", &import.tree, &mut aliases);
        }
    }
    aliases
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
    assert!(
        violations
            .iter()
            .any(|value| value.contains("scoped_negative.rs")),
        "capability detector lost an outer alias after inner shadowing: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|value| value.contains("scoped_positive.rs")),
        "capability detector leaked an inner alias into its parent scope: {violations:?}"
    );
}

#[test]
fn an_unbounded_channel_import_is_rejected() {
    let (root, _) = fixture_files("unbounded_channel");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec!["std::sync::mpsc::channel".to_owned()],
    }];
    let violations = capability_violations(&root, &rules);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("unbounded.rs") && value.contains("mpsc::channel")),
        "capability detector accepted a parent-imported unbounded channel: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("glob.rs") && value.contains("mpsc::channel")),
        "capability detector accepted an unbounded channel through a glob: {violations:?}"
    );
}
