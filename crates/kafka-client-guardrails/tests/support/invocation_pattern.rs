//! Pattern bindings that shadow invocation aliases within one lexical scope.

use syn::Pat;

use super::invocation_scope::{Binding, Scope};

pub(super) fn pattern_scope<'a>(patterns: impl Iterator<Item = &'a Pat>) -> Scope {
    let mut scope = Scope::new();
    for pattern in patterns {
        shadow_pattern(pattern, &mut scope);
    }
    scope
}

pub(super) fn pattern_names(pattern: &Pat) -> Vec<String> {
    let mut names = Vec::new();
    collect_pattern_names(pattern, &mut names);
    names
}

pub(super) fn shadow_pattern(pattern: &Pat, scope: &mut Scope) {
    for name in pattern_names(pattern) {
        scope.insert(name, Binding::Shadow);
    }
}

fn collect_pattern_names(pattern: &Pat, names: &mut Vec<String>) {
    match pattern {
        Pat::Ident(binding) => {
            names.push(binding.ident.to_string());
            if let Some((_, subpattern)) = &binding.subpat {
                collect_pattern_names(subpattern, names);
            }
        }
        Pat::Or(or) => {
            for case in &or.cases {
                collect_pattern_names(case, names);
            }
        }
        Pat::Paren(paren) => collect_pattern_names(&paren.pat, names),
        Pat::Reference(reference) => collect_pattern_names(&reference.pat, names),
        Pat::Slice(slice) => {
            for item in &slice.elems {
                collect_pattern_names(item, names);
            }
        }
        Pat::Struct(structure) => {
            for field in &structure.fields {
                collect_pattern_names(&field.pat, names);
            }
        }
        Pat::Tuple(tuple) => {
            for item in &tuple.elems {
                collect_pattern_names(item, names);
            }
        }
        Pat::TupleStruct(tuple) => {
            for item in &tuple.elems {
                collect_pattern_names(item, names);
            }
        }
        Pat::Type(typed) => collect_pattern_names(&typed.pat, names),
        _ => {}
    }
}
