//! AST recognition for async syntax that does not appear as a Rust path.

use syn::visit::{self, Visit};
use syn::{ExprAsync, ExprAwait, ExprClosure, File, Signature};

pub(super) fn contains_async(file: &File) -> bool {
    let mut visitor = AsyncVisitor { found: false };
    visitor.visit_file(file);
    visitor.found
}

struct AsyncVisitor {
    found: bool,
}

impl<'ast> Visit<'ast> for AsyncVisitor {
    fn visit_signature(&mut self, signature: &'ast Signature) {
        self.found |= signature.asyncness.is_some();
        visit::visit_signature(self, signature);
    }

    fn visit_expr_async(&mut self, expression: &'ast ExprAsync) {
        self.found = true;
        visit::visit_expr_async(self, expression);
    }

    fn visit_expr_await(&mut self, expression: &'ast ExprAwait) {
        self.found = true;
        visit::visit_expr_await(self, expression);
    }

    fn visit_expr_closure(&mut self, expression: &'ast ExprClosure) {
        self.found |= expression.asyncness.is_some();
        visit::visit_expr_closure(self, expression);
    }
}
