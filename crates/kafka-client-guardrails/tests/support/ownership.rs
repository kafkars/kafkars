//! AST inspection for protected field mutation and linear owner duplication.

use std::path::{Path, PathBuf};

use syn::{
    BinOp, Expr, ExprBinary, ExprMethodCall, ExprReference, File, ItemImpl, ItemStruct, Member,
    Type,
    visit::{self, Visit},
};

use super::{MutationOwner, display_path, read};

const MUTATING_METHODS: &[&str] = &[
    "append",
    "capture",
    "clear",
    "complete",
    "drain",
    "entry",
    "extend",
    "get_mut",
    "insert",
    "lock",
    "pop",
    "pop_back",
    "pop_front",
    "push",
    "push_back",
    "push_front",
    "release",
    "remove",
    "reserve",
    "store",
    "retain",
    "retain_committed_tail",
    "retain_generated",
    "retain_tail",
    "retain_terminal",
    "take",
    "take_effects",
    "take_generated",
    "clear_terminal",
    "try_reserve",
    "try_lock",
];

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
        if self.in_owner_impl && references_self_field(&expression.receiver, &self.rule.field) {
            if MUTATING_METHODS
                .iter()
                .any(|method| expression.method == method)
            {
                self.record_if_protected(&expression.receiver);
            } else if !matches!(expression.method.to_string().as_str(), "iter" | "len")
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
    fn record_if_protected(&mut self, expression: &Expr) {
        if !self.in_owner_impl || !references_self_field(expression, &self.rule.field) {
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
}

pub(crate) fn mutation_violations(
    root: &Path,
    files: &[PathBuf],
    rules: &[MutationOwner],
) -> Vec<String> {
    let parsed = files
        .iter()
        .map(|path| (display_path(root, path), parse(path)))
        .collect::<Vec<_>>();
    let mut violations = Vec::new();
    for rule in rules {
        for allowed in &rule.allowed_paths {
            if !root.join(allowed).is_file() {
                violations.push(format!("stale mutation owner path: {allowed}"));
            }
        }
        let mut declared = false;
        let mut mutations = 0;
        for (path, file) in &parsed {
            let mut visitor = MutationVisitor {
                rule,
                path,
                in_owner_impl: false,
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

fn references_self_field(expression: &Expr, field: &str) -> bool {
    match expression {
        Expr::Field(value) => {
            matches!(&value.member, Member::Named(name) if name == field)
                && matches!(&*value.base, Expr::Path(path) if path.path.is_ident("self"))
        }
        Expr::Group(value) => references_self_field(&value.expr, field),
        Expr::Index(value) => references_self_field(&value.expr, field),
        Expr::Paren(value) => references_self_field(&value.expr, field),
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

fn parse(path: &Path) -> File {
    syn::parse_file(&read(path)).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
