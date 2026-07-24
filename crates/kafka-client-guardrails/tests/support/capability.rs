//! AST-backed capability inspection for directory and exact-file roots.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use super::{CapabilityRule, WalkScope, async_capability, display_path, read, rust_files_under};
use syn::visit::Visit;
use syn::{Block, File, Item, ItemMod, ItemUse, Stmt, UseTree};

pub(crate) fn capability_violations(root: &Path, rules: &[CapabilityRule]) -> Vec<String> {
    let mut violations = Vec::new();
    for rule in rules {
        let source_root = root.join(&rule.root);
        assert!(
            source_root.exists(),
            "capability root {} is missing",
            source_root.display()
        );
        validate_allow_entries(root, &source_root, rule, &mut violations);
        let files = if source_root.is_file() {
            vec![source_root]
        } else {
            rust_files_under(&source_root, WalkScope::Fixture)
        };
        let mut used = BTreeSet::new();
        for path in files {
            inspect_file(root, &path, rule, &mut used, &mut violations);
        }
        for allowed in &rule.allow {
            if !used.contains(&(allowed.path.clone(), allowed.capability.clone())) {
                violations.push(format!(
                    "{} has decorative capability allow for {}",
                    allowed.path, allowed.capability
                ));
            }
        }
    }
    violations
}

fn validate_allow_entries(
    root: &Path,
    source_root: &Path,
    rule: &CapabilityRule,
    violations: &mut Vec<String>,
) {
    let mut unique = BTreeSet::new();
    for allowed in &rule.allow {
        let path = root.join(&allowed.path);
        if !path.is_file() || !path.starts_with(source_root) {
            violations.push(format!(
                "{} is not an exact file beneath capability root {}",
                allowed.path, rule.root
            ));
        }
        if !rule.forbidden.contains(&allowed.capability) {
            violations.push(format!(
                "{} allows undeclared capability {}",
                allowed.path, allowed.capability
            ));
        }
        if allowed.reason.trim().is_empty() {
            violations.push(format!(
                "{} allows {} without a reason",
                allowed.path, allowed.capability
            ));
        }
        if !unique.insert((&allowed.path, &allowed.capability)) {
            violations.push(format!(
                "{} duplicates capability allow {}",
                allowed.path, allowed.capability
            ));
        }
    }
}

fn inspect_file(
    root: &Path,
    path: &Path,
    rule: &CapabilityRule,
    used: &mut BTreeSet<(String, String)>,
    violations: &mut Vec<String>,
) {
    let source = read(path);
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, path)));
    let mut collector = CapabilityCollector::default();
    collector.visit_file(&syntax);
    if async_capability::contains_async(&syntax) {
        collector.paths.insert("async".into());
    }

    let relative = display_path(root, path);
    for observed in &collector.paths {
        for forbidden in &rule.forbidden {
            let reaches_forbidden = observed == forbidden
                || observed
                    .strip_prefix(forbidden)
                    .is_some_and(|suffix| suffix.starts_with("::"))
                || collector.globs.iter().any(|prefix| {
                    forbidden
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with("::"))
                });
            if !reaches_forbidden {
                continue;
            }
            if rule
                .allow
                .iter()
                .any(|allowed| allowed.path == relative && allowed.capability == *forbidden)
            {
                used.insert((relative.clone(), forbidden.clone()));
            } else {
                violations.push(format!(
                    "{relative} reaches forbidden capability {forbidden} through {observed}"
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

    fn visit_expr_method_call(&mut self, expression: &'ast syn::ExprMethodCall) {
        let method = expression.method.to_string();
        self.observe_path(&method);
        syn::visit::visit_expr_method_call(self, expression);
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
