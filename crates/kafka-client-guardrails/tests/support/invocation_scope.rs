//! Scoped Rust-name bindings used by structural invocation inspection.

use std::collections::{BTreeMap, BTreeSet};

use syn::{Block, Expr, FnArg, Item, Stmt, Type, UnOp, UseTree};

pub(super) use super::invocation_pattern::pattern_scope;
use super::invocation_pattern::{pattern_names, shadow_pattern};

#[derive(Clone)]
pub(super) enum Binding {
    Alias(String),
    Resolved(String),
    Unresolved(String),
    Shadow,
}

pub(super) type Scope = BTreeMap<String, Binding>;

pub(super) enum PathResolution {
    Resolved(String),
    Shadowed,
    Unresolved(String),
}

pub(super) fn item_scope(items: &[Item]) -> Scope {
    let mut scope = Scope::new();
    for item in items {
        record_item(item, &mut scope);
    }
    scope
}

pub(super) fn block_scope(block: &Block) -> Scope {
    let mut scope = Scope::new();
    for statement in &block.stmts {
        if let Stmt::Item(item) = statement {
            record_item(item, &mut scope);
        }
    }
    scope
}

pub(super) fn parameter_scope<'a>(inputs: impl Iterator<Item = &'a FnArg>) -> Scope {
    let mut scope = Scope::new();
    for input in inputs {
        match input {
            FnArg::Receiver(_) => {
                scope.insert("self".to_owned(), Binding::Shadow);
            }
            FnArg::Typed(argument) => shadow_pattern(&argument.pat, &mut scope),
        }
    }
    scope
}

pub(super) fn record_local(local: &syn::Local, scopes: &mut [Scope]) {
    let names = pattern_names(&local.pat);
    let resolved = local
        .init
        .as_ref()
        .and_then(|initializer| expression_path(&initializer.expr))
        .map(|path| resolve(scopes, &path_string(path)));
    let Some(scope) = scopes.last_mut() else {
        return;
    };
    if let [name] = names.as_slice() {
        match resolved {
            Some(PathResolution::Resolved(path)) => {
                scope.insert(name.clone(), Binding::Resolved(path));
                return;
            }
            Some(PathResolution::Unresolved(path)) => {
                scope.insert(name.clone(), Binding::Unresolved(path));
                return;
            }
            Some(PathResolution::Shadowed) | None => {}
        }
    }
    for name in names {
        scope.insert(name, Binding::Shadow);
    }
}

fn record_item(item: &Item, scope: &mut Scope) {
    match item {
        Item::Use(import) => collect_use_aliases("", &import.tree, scope),
        Item::Type(alias) => {
            let binding = type_path(&alias.ty).map_or(Binding::Shadow, Binding::Alias);
            scope.insert(alias.ident.to_string(), binding);
        }
        _ => {}
    }
}

pub(super) fn resolve_path(scopes: &[Scope], path: &syn::Path) -> PathResolution {
    resolve(scopes, &path_string(path))
}

fn resolve(scopes: &[Scope], path: &str) -> PathResolution {
    let mut segments = split_path(path);
    let mut visited = BTreeSet::new();
    let mut ceiling = scopes.len();
    loop {
        discard_relative_prefixes(&mut segments);
        let Some(first) = segments.first().cloned() else {
            return PathResolution::Unresolved(path.to_owned());
        };
        let Some((scope_index, binding)) = scopes[..ceiling]
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, scope)| scope.get(&first).map(|binding| (index, binding)))
        else {
            return PathResolution::Unresolved(segments.join("::"));
        };
        match binding {
            Binding::Shadow => return PathResolution::Shadowed,
            Binding::Resolved(target) => {
                let mut resolved = split_path(target);
                resolved.extend(segments.into_iter().skip(1));
                discard_relative_prefixes(&mut resolved);
                return PathResolution::Resolved(resolved.join("::"));
            }
            Binding::Unresolved(target) => {
                return PathResolution::Unresolved(target.clone());
            }
            Binding::Alias(target) => {
                if !visited.insert((scope_index, first, target.clone())) {
                    return PathResolution::Unresolved(path.to_owned());
                }
                if target.starts_with("crate::") {
                    let mut resolved = split_path(target);
                    resolved.extend(segments.into_iter().skip(1));
                    discard_relative_prefixes(&mut resolved);
                    return PathResolution::Resolved(resolved.join("::"));
                }
                ceiling = alias_ceiling(target, scope_index, scopes.len());
                let mut replacement = split_path(target);
                replacement.extend(segments.into_iter().skip(1));
                segments = replacement;
            }
        }
    }
}

fn alias_ceiling(target: &str, scope_index: usize, scope_count: usize) -> usize {
    match target.split("::").next() {
        Some("crate") => 1.min(scope_count),
        Some("super") => scope_index,
        _ => (scope_index + 1).min(scope_count),
    }
}

fn expression_path(expression: &Expr) -> Option<&syn::Path> {
    match expression {
        Expr::Cast(cast) => expression_path(&cast.expr),
        Expr::Group(group) => expression_path(&group.expr),
        Expr::Paren(paren) => expression_path(&paren.expr),
        Expr::Path(path) => Some(&path.path),
        Expr::Reference(reference) => expression_path(&reference.expr),
        Expr::Unary(unary) if matches!(unary.op, UnOp::Deref(_)) => expression_path(&unary.expr),
        _ => None,
    }
}

fn type_path(alias: &Type) -> Option<String> {
    match alias {
        Type::Group(group) => type_path(&group.elem),
        Type::Paren(paren) => type_path(&paren.elem),
        Type::Path(path) => Some(path_string(&path.path)),
        _ => None,
    }
}

fn collect_use_aliases(prefix: &str, tree: &UseTree, scope: &mut Scope) {
    match tree {
        UseTree::Path(path) => {
            let next = append_segment(prefix, &path.ident.to_string());
            collect_use_aliases(&next, &path.tree, scope);
        }
        UseTree::Name(name) => {
            let name = name.ident.to_string();
            let target = if name == "self" {
                prefix.to_owned()
            } else {
                append_segment(prefix, &name)
            };
            let local = if name == "self" {
                prefix.rsplit("::").next().unwrap_or_default().to_owned()
            } else {
                name
            };
            scope.insert(local, Binding::Alias(target));
        }
        UseTree::Rename(rename) => {
            scope.insert(
                rename.rename.to_string(),
                Binding::Alias(append_segment(prefix, &rename.ident.to_string())),
            );
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_aliases(prefix, item, scope);
            }
        }
        UseTree::Glob(_) => {}
    }
}

fn path_string(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

fn split_path(path: &str) -> Vec<String> {
    path.split("::").map(str::to_owned).collect()
}

fn discard_relative_prefixes(segments: &mut Vec<String>) {
    while segments.len() > 1
        && segments
            .first()
            .is_some_and(|segment| matches!(segment.as_str(), "crate" | "self" | "super"))
    {
        segments.remove(0);
    }
}

fn append_segment(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_owned()
    } else {
        format!("{prefix}::{segment}")
    }
}
