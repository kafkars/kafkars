//! Scoped AST resolution for direct, aliased, and UFCS invocations.

use std::collections::BTreeSet;

use syn::{
    Block, ExprClosure, File, ImplItemFn, ItemFn, ItemMod, Stmt, TraitItemFn,
    visit::{self, Visit},
};

use super::invocation_scope::{
    PathResolution, Scope, block_scope, item_scope, parameter_scope, pattern_scope, record_local,
    resolve_path,
};
use super::macro_identifiers;

pub(crate) struct InvocationEvidence {
    pub(crate) paths: BTreeSet<String>,
    pub(crate) unresolved: BTreeSet<String>,
    pub(crate) macro_identifiers: BTreeSet<String>,
}

pub(crate) fn invocations(file: &File) -> InvocationEvidence {
    let mut collector = InvocationCollector::default();
    collector.visit_file(file);
    InvocationEvidence {
        paths: collector.observed,
        unresolved: collector.unresolved,
        macro_identifiers: collector.macro_identifiers,
    }
}

pub(crate) fn invocation_matches(observed: &str, expected: &str) -> bool {
    observed == expected
        || observed
            .strip_suffix(expected)
            .is_some_and(|prefix| prefix.ends_with("::"))
}

pub(crate) fn invocation_candidate_matches(observed: &str, expected: &str) -> bool {
    observed
        .rsplit("::")
        .next()
        .zip(expected.rsplit("::").next())
        .is_some_and(|(observed, expected)| observed == expected)
}

#[derive(Default)]
struct InvocationCollector {
    observed: BTreeSet<String>,
    unresolved: BTreeSet<String>,
    macro_identifiers: BTreeSet<String>,
    scopes: Vec<Scope>,
}

impl<'ast> Visit<'ast> for InvocationCollector {
    fn visit_file(&mut self, file: &'ast File) {
        self.scopes.push(item_scope(&file.items));
        for item in &file.items {
            self.visit_item(item);
        }
        self.scopes.pop();
    }

    fn visit_item_mod(&mut self, module: &'ast ItemMod) {
        let Some((_, items)) = &module.content else {
            return;
        };
        self.scopes.push(item_scope(items));
        for item in items {
            self.visit_item(item);
        }
        self.scopes.pop();
    }

    fn visit_block(&mut self, block: &'ast Block) {
        self.scopes.push(block_scope(block));
        for statement in &block.stmts {
            match statement {
                Stmt::Local(local) => {
                    visit::visit_local(self, local);
                    record_local(local, &mut self.scopes);
                }
                _ => self.visit_stmt(statement),
            }
        }
        self.scopes.pop();
    }

    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        match resolve_path(&self.scopes, &expression.path) {
            PathResolution::Resolved(path) => {
                self.observed.insert(path);
            }
            PathResolution::Unresolved(path) => {
                self.unresolved.insert(path);
            }
            PathResolution::Shadowed => {}
        }
        visit::visit_expr_path(self, expression);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        self.observed.insert(call.method.to_string());
        visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, value: &'ast syn::Macro) {
        self.macro_identifiers.extend(macro_identifiers(value));
        visit::visit_macro(self, value);
    }

    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        self.scopes
            .push(parameter_scope(function.sig.inputs.iter()));
        self.visit_block(&function.block);
        self.scopes.pop();
    }

    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        self.scopes
            .push(parameter_scope(function.sig.inputs.iter()));
        self.visit_block(&function.block);
        self.scopes.pop();
    }

    fn visit_trait_item_fn(&mut self, function: &'ast TraitItemFn) {
        let Some(block) = &function.default else {
            return;
        };
        self.scopes
            .push(parameter_scope(function.sig.inputs.iter()));
        self.visit_block(block);
        self.scopes.pop();
    }

    fn visit_expr_closure(&mut self, closure: &'ast ExprClosure) {
        self.scopes.push(pattern_scope(closure.inputs.iter()));
        self.visit_expr(&closure.body);
        self.scopes.pop();
    }
}
