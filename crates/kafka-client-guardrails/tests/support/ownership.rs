//! AST inspection for protected field mutation and linear owner duplication.
use super::{
    MutationOwner, display_path, is_test_only_source,
    ownership_methods::{is_mutating_method, is_non_owning_access},
    read,
};
use std::path::{Path, PathBuf};
use syn::{
    BinOp, Expr, ExprBinary, ExprMethodCall, ExprReference, FnArg, ImplItemFn, ItemFn, ItemImpl,
    ItemStruct, Member, Pat, Type,
    visit::{self, Visit},
};
#[derive(Default)]
struct MutationEvidence {
    declared: bool,
    mutations: usize,
    violations: Vec<String>,
}
struct MutationVisitor<'a> {
    rule: &'a MutationOwner,
    path: &'a str,
    in_owner_impl: bool,
    track_owner_bindings: bool,
    owner_bindings: Vec<String>,
    evidence: MutationEvidence,
}
impl<'ast> Visit<'ast> for MutationVisitor<'_> {
    fn visit_item_struct(&mut self, item: &'ast ItemStruct) {
        if item.ident == self.rule.owner_type
            && item.fields.iter().any(|field| {
                field
                    .ident
                    .as_ref()
                    .is_some_and(|name| name == self.rule.field.as_str())
            })
        {
            self.evidence.declared = true;
        }
        visit::visit_item_struct(self, item);
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        let previous = self.in_owner_impl;
        self.in_owner_impl = type_is(&item.self_ty, &self.rule.owner_type);
        visit::visit_item_impl(self, item);
        self.in_owner_impl = previous;
    }

    fn visit_item_fn(&mut self, item: &'ast ItemFn) {
        self.visit_function(&item.sig.inputs, |visitor| {
            visit::visit_item_fn(visitor, item);
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast ImplItemFn) {
        self.visit_function(&item.sig.inputs, |visitor| {
            visit::visit_impl_item_fn(visitor, item);
        });
    }

    fn visit_expr_assign(&mut self, expression: &'ast syn::ExprAssign) {
        self.record_if_protected(&expression.left);
        visit::visit_expr_assign(self, expression);
    }

    fn visit_expr_binary(&mut self, expression: &'ast ExprBinary) {
        if is_assign_op(&expression.op) {
            self.record_if_protected(&expression.left);
        }
        visit::visit_expr_binary(self, expression);
    }

    fn visit_expr_reference(&mut self, expression: &'ast ExprReference) {
        if expression.mutability.is_some() {
            self.record_if_protected(&expression.expr);
        }
        visit::visit_expr_reference(self, expression);
    }

    fn visit_expr_method_call(&mut self, expression: &'ast ExprMethodCall) {
        if self.references_owner_field(&expression.receiver) {
            if is_mutating_method(&expression.method) {
                self.record_if_protected(&expression.receiver);
            } else if !is_non_owning_access(self.rule, &expression.method)
                && !self.rule.allowed_paths.iter().any(|path| path == self.path)
            {
                self.evidence.violations.push(format!(
                    "{} directly accesses {}.{} outside its configured owner modules",
                    self.path, self.rule.owner_type, self.rule.field
                ));
            }
        }
        visit::visit_expr_method_call(self, expression);
    }
}
impl MutationVisitor<'_> {
    fn visit_function(
        &mut self,
        inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
        visit_function: impl FnOnce(&mut Self),
    ) {
        let bindings = if self.track_owner_bindings {
            owner_bindings(inputs, &self.rule.owner_type)
        } else {
            Vec::new()
        };
        let previous = std::mem::replace(&mut self.owner_bindings, bindings);
        visit_function(self);
        self.owner_bindings = previous;
    }

    fn record_if_protected(&mut self, expression: &Expr) {
        if !self.references_owner_field(expression) {
            return;
        }
        self.evidence.mutations += 1;
        if !self.rule.allowed_paths.iter().any(|path| path == self.path) {
            self.evidence.violations.push(format!(
                "{} mutates {}.{} outside its configured owner modules",
                self.path, self.rule.owner_type, self.rule.field
            ));
        }
    }

    fn references_owner_field(&self, expression: &Expr) -> bool {
        references_owner_field(
            expression,
            &self.rule.field,
            self.in_owner_impl,
            &self.owner_bindings,
        )
    }
}

pub(crate) fn mutation_violations(
    root: &Path,
    files: &[PathBuf],
    rules: &[MutationOwner],
) -> Vec<String> {
    let parsed = files
        .iter()
        .map(|path| {
            let source = read(path);
            (
                display_path(root, path),
                syn::parse_file(&source)
                    .unwrap_or_else(|error| panic!("parse {}: {error}", path.display())),
                !is_test_only_source(path),
                source,
            )
        })
        .collect::<Vec<_>>();
    let mut violations = Vec::new();
    for rule in rules {
        for allowed in &rule.allowed_paths {
            if !root.join(allowed).is_file() {
                violations.push(format!("stale mutation owner path: {allowed}"));
            }
        }
        let (mut declared, mut mutations) = (false, 0);
        for (path, file, track_owner_bindings, source) in &parsed {
            // Every declaration, owner impl, and typed owner binding recognized
            // below names both the protected owner type and its field. Avoid a
            // full AST visit when the source cannot contribute evidence.
            if !source.contains(&rule.owner_type) || !source.contains(&rule.field) {
                continue;
            }
            let mut visitor = MutationVisitor {
                rule,
                path,
                in_owner_impl: false,
                track_owner_bindings: *track_owner_bindings,
                owner_bindings: Vec::new(),
                evidence: MutationEvidence::default(),
            };
            visitor.visit_file(file);
            declared |= visitor.evidence.declared;
            mutations += visitor.evidence.mutations;
            violations.extend(visitor.evidence.violations);
        }
        if !declared {
            violations.push(format!(
                "stale mutation rule: {}.{} is not declared",
                rule.owner_type, rule.field
            ));
        } else if mutations == 0 {
            violations.push(format!(
                "decorative mutation rule: {}.{} has no detected mutations",
                rule.owner_type, rule.field
            ));
        }
    }
    violations
}

fn references_owner_field(
    expression: &Expr,
    field: &str,
    in_owner_impl: bool,
    owner_bindings: &[String],
) -> bool {
    match expression {
        Expr::Field(value) => {
            matches!(&value.member, Member::Named(name) if name == field)
                && matches!(
                    &*value.base,
                    Expr::Path(path)
                        if (in_owner_impl && path.path.is_ident("self"))
                            || owner_bindings
                                .iter()
                                .any(|binding| path.path.is_ident(binding.as_str()))
                )
        }
        Expr::Group(value) => {
            references_owner_field(&value.expr, field, in_owner_impl, owner_bindings)
        }
        Expr::Index(value) => {
            references_owner_field(&value.expr, field, in_owner_impl, owner_bindings)
        }
        Expr::Paren(value) => {
            references_owner_field(&value.expr, field, in_owner_impl, owner_bindings)
        }
        _ => false,
    }
}

fn owner_bindings(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
    owner_type: &str,
) -> Vec<String> {
    inputs
        .iter()
        .filter_map(|argument| {
            let FnArg::Typed(argument) = argument else {
                return None;
            };
            if !type_reaches(&argument.ty, owner_type) {
                return None;
            }
            let Pat::Ident(binding) = &*argument.pat else {
                return None;
            };
            Some(binding.ident.to_string())
        })
        .collect()
}

fn type_reaches(value: &Type, expected: &str) -> bool {
    match value {
        Type::Group(value) => type_reaches(&value.elem, expected),
        Type::Paren(value) => type_reaches(&value.elem, expected),
        Type::Path(_) => type_is(value, expected),
        Type::Reference(value) => type_reaches(&value.elem, expected),
        _ => false,
    }
}

fn type_is(value: &Type, expected: &str) -> bool {
    let Type::Path(path) = value else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == expected)
}

const fn is_assign_op(operator: &BinOp) -> bool {
    matches!(
        operator,
        BinOp::AddAssign(_)
            | BinOp::SubAssign(_)
            | BinOp::MulAssign(_)
            | BinOp::DivAssign(_)
            | BinOp::RemAssign(_)
            | BinOp::BitXorAssign(_)
            | BinOp::BitAndAssign(_)
            | BinOp::BitOrAssign(_)
            | BinOp::ShlAssign(_)
            | BinOp::ShrAssign(_)
    )
}
