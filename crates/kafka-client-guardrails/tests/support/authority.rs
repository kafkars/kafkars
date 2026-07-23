//! AST inspection for private, owner-constructed authority tokens.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use syn::{
    BinOp, Expr, ExprBinary, ExprReference, ItemStruct, Member, Visibility,
    visit::{self, Visit},
};

use super::{AuthorityToken, display_path, is_test_only_source, macro_identifiers, read};

pub(crate) fn authority_token_violations(
    root: &Path,
    files: &[PathBuf],
    rules: &[AuthorityToken],
) -> Vec<String> {
    let parsed = files
        .iter()
        .filter(|path| !is_test_source(root, path))
        .map(|path| (display_path(root, path), parse(path)))
        .collect::<Vec<_>>();
    let mut violations = Vec::new();
    for rule in rules {
        validate_declaration(root, rule, &mut violations);
        if rule.allowed_paths.len() != 1 || rule.allowed_paths[0] != rule.path {
            violations.push(format!(
                "authority {} construction must be owned only by {}",
                rule.owner_type, rule.path
            ));
        }
        for (path, file) in &parsed {
            let mut visitor = AuthorityVisitor {
                rule,
                path,
                allowed: rule.allowed_paths.iter().any(|allowed| allowed == path),
                violations: Vec::new(),
            };
            visitor.visit_file(file);
            violations.extend(visitor.violations);
        }
    }
    violations
}

fn is_test_source(root: &Path, path: &Path) -> bool {
    is_test_only_source(path) || display_path(root, path).contains("/tests/")
}

fn validate_declaration(root: &Path, rule: &AuthorityToken, violations: &mut Vec<String>) {
    let path = root.join(&rule.path);
    if !path.is_file() {
        violations.push(format!("stale authority declaration path: {}", rule.path));
        return;
    }
    let file = parse(&path);
    if file
        .items
        .iter()
        .any(|item| matches!(item, syn::Item::Mod(_)))
    {
        violations.push(format!("{} is not a leaf authority module", rule.path));
    }
    let declaration = file.items.iter().find_map(|item| match item {
        syn::Item::Struct(value) if value.ident == rule.owner_type => Some(value),
        _ => None,
    });
    let Some(declaration) = declaration else {
        violations.push(format!(
            "stale authority rule: {} is not declared in {}",
            rule.owner_type, rule.path
        ));
        return;
    };
    if matches!(declaration.vis, Visibility::Public(_)) {
        violations.push(format!(
            "{} exposes authority {} beyond crate visibility",
            rule.path, rule.owner_type
        ));
    }
    validate_fields(rule, declaration, violations);
}

fn validate_fields(rule: &AuthorityToken, declaration: &ItemStruct, violations: &mut Vec<String>) {
    let configured = rule.fields.iter().cloned().collect::<BTreeSet<_>>();
    let actual = declaration
        .fields
        .iter()
        .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
        .collect::<BTreeSet<_>>();
    if actual != configured {
        violations.push(format!(
            "{} authority {} fields differ: configured={configured:?}, declared={actual:?}",
            rule.path, rule.owner_type
        ));
    }
    for field in &declaration.fields {
        if !matches!(field.vis, Visibility::Inherited) {
            violations.push(format!(
                "{} exposes a non-private field on authority {}",
                rule.path, rule.owner_type
            ));
        }
    }
}

struct AuthorityVisitor<'a> {
    rule: &'a AuthorityToken,
    path: &'a str,
    allowed: bool,
    violations: Vec<String>,
}

impl<'ast> Visit<'ast> for AuthorityVisitor<'_> {
    fn visit_expr_struct(&mut self, expression: &'ast syn::ExprStruct) {
        let protected_fields = expression
            .fields
            .iter()
            .filter_map(|field| match &field.member {
                Member::Named(name) if self.rule.fields.iter().any(|field| name == field) => {
                    Some(name.to_string())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if !self.allowed
            && (path_is(&expression.path, &self.rule.owner_type) || !protected_fields.is_empty())
        {
            self.violations.push(format!(
                "{} constructs authority {} outside its owner module using fields {:?}",
                self.path, self.rule.owner_type, protected_fields
            ));
        }
        visit::visit_expr_struct(self, expression);
    }

    fn visit_expr_assign(&mut self, expression: &'ast syn::ExprAssign) {
        self.record_mutation(&expression.left);
        visit::visit_expr_assign(self, expression);
    }

    fn visit_expr_binary(&mut self, expression: &'ast ExprBinary) {
        if is_assign_op(&expression.op) {
            self.record_mutation(&expression.left);
        }
        visit::visit_expr_binary(self, expression);
    }

    fn visit_expr_reference(&mut self, expression: &'ast ExprReference) {
        if expression.mutability.is_some() {
            self.record_mutation(&expression.expr);
        }
        visit::visit_expr_reference(self, expression);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        if !self.allowed {
            let identifiers = macro_identifiers(value);
            if identifiers.contains(&self.rule.owner_type)
                || self
                    .rule
                    .fields
                    .iter()
                    .any(|field| identifiers.contains(field))
            {
                self.violations.push(format!(
                    "{} contains authority {} tokens inside a macro",
                    self.path, self.rule.owner_type
                ));
            }
        }
        visit::visit_macro(self, value);
    }
}

impl AuthorityVisitor<'_> {
    fn record_mutation(&mut self, expression: &Expr) {
        let Some(field) = references_field(expression, &self.rule.fields) else {
            return;
        };
        if self.allowed {
            return;
        }
        self.violations.push(format!(
            "{} mutates authority {}.{} outside its owner module",
            self.path, self.rule.owner_type, field
        ));
    }
}

fn references_field(expression: &Expr, fields: &[String]) -> Option<String> {
    match expression {
        Expr::Field(value) => match &value.member {
            Member::Named(name) if fields.iter().any(|field| name == field) => {
                Some(name.to_string())
            }
            _ => None,
        },
        Expr::Group(value) => references_field(&value.expr, fields),
        Expr::Index(value) => references_field(&value.expr, fields),
        Expr::Paren(value) => references_field(&value.expr, fields),
        _ => None,
    }
}

fn path_is(path: &syn::Path, expected: &str) -> bool {
    path.segments
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

fn parse(path: &Path) -> syn::File {
    syn::parse_file(&read(path)).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}
