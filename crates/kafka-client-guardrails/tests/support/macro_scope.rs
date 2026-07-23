//! Provenance tracking for macro names trusted by source-graph inspection.

use std::collections::BTreeSet;

use syn::visit::Visit;
use syn::{Attribute, ItemExternCrate, ItemMod, ItemUse, UseTree};

#[derive(Default)]
pub(crate) struct MacroScope {
    imported_names: BTreeSet<String>,
    trusted_imports: BTreeSet<String>,
    shadowed_roots: BTreeSet<String>,
    has_glob_import: bool,
    has_opaque_macro_import: bool,
}

impl MacroScope {
    pub(crate) fn inspect(syntax: &syn::File) -> Self {
        let mut collector = MacroScopeCollector::default();
        collector.visit_file(syntax);
        let trusted_imports = collector
            .trusted_import_roots
            .iter()
            .filter(|(_, root)| {
                !collector.has_glob_import && !collector.shadowed_roots.contains(root)
            })
            .map(|(name, _)| name.clone())
            .collect();
        Self {
            imported_names: collector.imported_names,
            trusted_imports,
            shadowed_roots: collector.shadowed_roots,
            has_glob_import: collector.has_glob_import,
            has_opaque_macro_import: collector.has_opaque_macro_import,
        }
    }

    pub(crate) fn trusts(&self, path: &syn::Path, name: &str) -> bool {
        if !safe_builtin_macro(name) {
            return false;
        }
        if path.segments.len() > 1 {
            return path
                .segments
                .first()
                .map(|segment| segment.ident.to_string())
                .is_some_and(|root| {
                    !self.has_glob_import
                        && !self.shadowed_roots.contains(&root)
                        && trusted_root_macro(&root, name)
                });
        }
        self.trusted_imports.contains(name)
            || !self.imported_names.contains(name) && !self.has_glob_import
    }

    pub(crate) fn permits_local(&self, name: &str) -> bool {
        !self.imported_names.contains(name) && !self.has_glob_import
    }

    pub(crate) fn has_opaque_macro_import(&self) -> bool {
        self.has_opaque_macro_import
    }
}

#[derive(Default)]
struct MacroScopeCollector {
    imported_names: BTreeSet<String>,
    trusted_import_roots: Vec<(String, String)>,
    shadowed_roots: BTreeSet<String>,
    has_glob_import: bool,
    has_opaque_macro_import: bool,
}

impl<'ast> Visit<'ast> for MacroScopeCollector {
    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        collect_imports(
            &item.tree,
            &mut Vec::new(),
            item.leading_colon.is_some(),
            &mut self.imported_names,
            &mut self.trusted_import_roots,
            &mut self.shadowed_roots,
            &mut self.has_glob_import,
        );
        syn::visit::visit_item_use(self, item);
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        mark_shadowed_root(&item.ident.to_string(), &mut self.shadowed_roots);
        if has_macro_use(&item.attrs) {
            self.has_glob_import = true;
            self.has_opaque_macro_import = true;
        }
        syn::visit::visit_item_mod(self, item);
    }

    fn visit_item_extern_crate(&mut self, item: &'ast ItemExternCrate) {
        let original = item.ident.to_string();
        let bound = item
            .rename
            .as_ref()
            .map_or_else(|| original.clone(), |(_, name)| name.to_string());
        if original != bound {
            mark_shadowed_root(&bound, &mut self.shadowed_roots);
        }
        if has_macro_use(&item.attrs) {
            self.has_glob_import = true;
            self.has_opaque_macro_import = true;
        }
        syn::visit::visit_item_extern_crate(self, item);
    }
}

fn has_macro_use(attributes: &[Attribute]) -> bool {
    attributes
        .iter()
        .any(|attribute| attribute.path().is_ident("macro_use"))
}

fn collect_imports(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    absolute: bool,
    names: &mut BTreeSet<String>,
    trusted: &mut Vec<(String, String)>,
    shadowed_roots: &mut BTreeSet<String>,
    has_glob: &mut bool,
) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_imports(
                &path.tree,
                prefix,
                absolute,
                names,
                trusted,
                shadowed_roots,
                has_glob,
            );
            prefix.pop();
        }
        UseTree::Name(name) => collect_import_leaf(
            prefix,
            absolute,
            &name.ident.to_string(),
            &name.ident.to_string(),
            names,
            trusted,
            shadowed_roots,
        ),
        UseTree::Rename(rename) => collect_import_leaf(
            prefix,
            absolute,
            &rename.ident.to_string(),
            &rename.rename.to_string(),
            names,
            trusted,
            shadowed_roots,
        ),
        UseTree::Glob(_) => *has_glob = true,
        UseTree::Group(group) => {
            for item in &group.items {
                collect_imports(
                    item,
                    prefix,
                    absolute,
                    names,
                    trusted,
                    shadowed_roots,
                    has_glob,
                );
            }
        }
    }
}

fn collect_import_leaf(
    prefix: &[String],
    absolute: bool,
    original: &str,
    requested_bound: &str,
    names: &mut BTreeSet<String>,
    trusted: &mut Vec<(String, String)>,
    shadowed_roots: &mut BTreeSet<String>,
) {
    let bound = if original == "self" && requested_bound == "self" {
        prefix.last().map_or(requested_bound, String::as_str)
    } else {
        requested_bound
    };
    let source = if original == "self" {
        prefix.to_vec()
    } else {
        prefix
            .iter()
            .cloned()
            .chain(std::iter::once(original.to_owned()))
            .collect()
    };
    if let [root, macro_name] = source.as_slice()
        && bound == macro_name
        && trusted_root_macro(root, macro_name)
    {
        trusted.push((bound.to_owned(), root.clone()));
    }
    if trusted_root(bound) && !(absolute && source == [bound]) {
        shadowed_roots.insert(bound.to_owned());
    }
    names.insert(bound.to_owned());
}

fn mark_shadowed_root(name: &str, roots: &mut BTreeSet<String>) {
    if trusted_root(name) {
        roots.insert(name.to_owned());
    }
}

fn trusted_root(name: &str) -> bool {
    matches!(name, "std" | "core" | "syn")
}

fn trusted_root_macro(root: &str, name: &str) -> bool {
    matches!(root, "std" | "core") && safe_builtin_macro(name) || root == "syn" && name == "Token"
}

pub(crate) fn source_capable_definition(tokens: &str) -> bool {
    [
        "mod", "path", "item", "meta", "tt", "ident", "block", "stmt", "include", "!",
    ]
    .iter()
    .any(|candidate| token_is_identifier(tokens, candidate))
}

fn token_is_identifier(tokens: &str, candidate: &str) -> bool {
    tokens.split_whitespace().any(|token| token == candidate)
}

fn safe_builtin_macro(name: &str) -> bool {
    matches!(
        name,
        "Token"
            | "assert"
            | "assert_eq"
            | "assert_ne"
            | "cfg"
            | "column"
            | "compile_error"
            | "concat"
            | "debug_assert"
            | "debug_assert_eq"
            | "debug_assert_ne"
            | "dbg"
            | "eprint"
            | "eprintln"
            | "env"
            | "file"
            | "format"
            | "format_args"
            | "include_bytes"
            | "include_str"
            | "line"
            | "matches"
            | "module_path"
            | "option_env"
            | "panic"
            | "print"
            | "println"
            | "stringify"
            | "thread_local"
            | "todo"
            | "unimplemented"
            | "unreachable"
            | "vec"
            | "write"
            | "writeln"
    )
}
