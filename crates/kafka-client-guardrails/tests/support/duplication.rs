//! Workspace-wide duplication bans plus registered linear-owner checks.

use std::path::{Path, PathBuf};

use syn::{
    Attribute, ItemImpl, ItemUse, UseTree,
    visit::{self, Visit},
};

use super::{
    AuthorityToken, LinearOwner, display_path, is_test_only_source, macro_identifiers, read,
};

pub(crate) fn linear_violations(
    root: &Path,
    files: &[PathBuf],
    rules: &[LinearOwner],
) -> Vec<String> {
    let mut violations = manual_duplication_violations(root, files);
    for rule in rules {
        let path = root.join(&rule.path);
        if !path.is_file() {
            violations.push(format!("stale linear-owner path: {}", rule.path));
            continue;
        }
        let file = parse(&path);
        let attributes = file.items.iter().find_map(|item| match item {
            syn::Item::Struct(value) if value.ident == rule.owner_type => Some(&value.attrs),
            syn::Item::Enum(value) if value.ident == rule.owner_type => Some(&value.attrs),
            _ => None,
        });
        let Some(attributes) = attributes else {
            violations.push(format!(
                "stale linear-owner rule: {} is not declared in {}",
                rule.owner_type, rule.path
            ));
            continue;
        };
        for forbidden in forbidden_derives(attributes) {
            violations.push(format!(
                "{} derives {forbidden} for linear owner {}",
                rule.path, rule.owner_type
            ));
        }
    }
    violations
}

pub(crate) fn authority_linear_violations(
    authorities: &[AuthorityToken],
    linear: &[LinearOwner],
) -> Vec<String> {
    let mut violations = Vec::new();
    for authority in authorities {
        let matches = linear
            .iter()
            .filter(|owner| {
                owner.owner_type == authority.owner_type && owner.path == authority.path
            })
            .count();
        if matches != 1 {
            violations.push(format!(
                "authority {} must have exactly one matching linear-owner rule",
                authority.owner_type
            ));
        }
    }
    violations
}

fn manual_duplication_violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files.iter().filter(|path| !is_test_source(root, path)) {
        let file = parse(path);
        let mut visitor = DuplicationVisitor {
            path: display_path(root, path),
            violations: Vec::new(),
        };
        visitor.visit_file(&file);
        violations.extend(visitor.violations);
    }
    violations
}

fn is_test_source(root: &Path, path: &Path) -> bool {
    is_test_only_source(path) || display_path(root, path).contains("/tests/")
}

struct DuplicationVisitor {
    path: String,
    violations: Vec<String>,
}

impl<'ast> Visit<'ast> for DuplicationVisitor {
    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        if let Some((_, path, _)) = &item.trait_
            && let Some(trait_name) = path.segments.last()
            && is_duplication_trait(&trait_name.ident.to_string())
        {
            self.violations.push(format!(
                "{} manually implements {} in production",
                self.path, trait_name.ident
            ));
        }
        visit::visit_item_impl(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        if imports_duplication_trait(&item.tree) {
            self.violations.push(format!(
                "{} imports or renames Clone/Copy in production",
                self.path
            ));
        }
        visit::visit_item_use(self, item);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        if macro_identifiers(value)
            .iter()
            .any(|identifier| is_duplication_trait(identifier))
        {
            self.violations.push(format!(
                "{} contains Clone/Copy inside opaque macro tokens",
                self.path
            ));
        }
        visit::visit_macro(self, value);
    }
}

fn imports_duplication_trait(tree: &UseTree) -> bool {
    match tree {
        UseTree::Name(name) => is_duplication_trait(&name.ident.to_string()),
        UseTree::Rename(rename) => is_duplication_trait(&rename.ident.to_string()),
        UseTree::Path(path) => imports_duplication_trait(&path.tree),
        UseTree::Group(group) => group.items.iter().any(imports_duplication_trait),
        UseTree::Glob(_) => false,
    }
}

fn forbidden_derives(attributes: &[Attribute]) -> Vec<String> {
    let mut forbidden = Vec::new();
    for attribute in attributes
        .iter()
        .filter(|value| value.path().is_ident("derive"))
    {
        attribute
            .parse_nested_meta(|meta| {
                if let Some(segment) = meta.path.segments.last()
                    && is_duplication_trait(&segment.ident.to_string())
                {
                    forbidden.push(segment.ident.to_string());
                }
                Ok(())
            })
            .unwrap_or_else(|error| panic!("parse derive attribute: {error}"));
    }
    forbidden
}

fn is_duplication_trait(value: &str) -> bool {
    matches!(value, "Clone" | "Copy")
}

fn parse(path: &Path) -> syn::File {
    syn::parse_file(&read(path)).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
