//! Structural guard against implementation glob imports.

use std::path::{Path, PathBuf};

use syn::{ItemUse, UseTree, Visibility, visit::Visit};

use super::{display_path, is_facade, is_test_only_source, read};

pub(crate) fn glob_import_violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files.iter().filter(|path| !is_test_only_source(path)) {
        let source = read(path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, path)));
        let mut visitor = GlobImportVisitor {
            facade: is_facade(path),
            path: display_path(root, path),
            violations: Vec::new(),
        };
        visitor.visit_file(&syntax);
        violations.extend(visitor.violations);
    }
    violations
}

struct GlobImportVisitor {
    facade: bool,
    path: String,
    violations: Vec<String>,
}

impl<'ast> Visit<'ast> for GlobImportVisitor {
    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        if has_glob(&item.tree) && !allowed_facade_reexport(self.facade, item) {
            self.violations.push(format!(
                "{} contains a glob import outside a public facade re-export",
                self.path
            ));
        }
        syn::visit::visit_item_use(self, item);
    }
}

fn allowed_facade_reexport(facade: bool, item: &ItemUse) -> bool {
    if !facade {
        return false;
    }
    if matches!(item.vis, Visibility::Public(_)) {
        return true;
    }
    let Visibility::Restricted(restricted) = &item.vis else {
        return false;
    };
    restricted.in_token.is_none()
        && restricted.path.is_ident("crate")
        && is_curated_exports_glob(&item.tree)
}

fn is_curated_exports_glob(tree: &UseTree) -> bool {
    let UseTree::Path(path) = tree else {
        return false;
    };
    path.ident == "exports" && matches!(path.tree.as_ref(), UseTree::Glob(_))
}

fn has_glob(tree: &UseTree) -> bool {
    match tree {
        UseTree::Glob(_) => true,
        UseTree::Group(group) => group.items.iter().any(has_glob),
        UseTree::Path(path) => has_glob(&path.tree),
        UseTree::Name(_) | UseTree::Rename(_) => false,
    }
}
