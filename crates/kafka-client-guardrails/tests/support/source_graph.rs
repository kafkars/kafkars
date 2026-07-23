//! Reachable Rust module inspection from bounded Cargo target roots.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use syn::punctuated::Punctuated;
use syn::{Attribute, Item, Meta, Token};

use super::{
    PackageTargets, display_path, rust_source_expansion_violation, valid_relative_policy_path,
    workspace_targets,
};

pub(crate) fn workspace_source_violations(workspace: &Path, inspected: &[PathBuf]) -> Vec<String> {
    let (packages, mut violations) = workspace_targets(workspace);
    let mut reachable = BTreeSet::new();
    for package in &packages {
        let mut graph = SourceGraph::new(workspace, &package.package_root);
        for target in &package.target_roots {
            let module_dir = target.parent().unwrap_or(&package.package_root);
            graph.walk_file(target, module_dir);
        }
        reachable.append(&mut graph.reachable);
        violations.append(&mut graph.violations);
    }
    let package_roots = packages
        .iter()
        .map(|package| package.package_root.as_path())
        .collect::<Vec<_>>();
    let inspected = inspected.iter().cloned().collect::<BTreeSet<_>>();
    for path in &reachable {
        if !inspected.contains(path) {
            violations.push(format!(
                "{} is reachable but omitted from guardrail inspection",
                display_path(workspace, path)
            ));
        }
    }
    for path in &inspected {
        if let Some(violation) = rust_source_expansion_violation(workspace, path) {
            violations.push(violation);
        }
        if !package_roots.iter().any(|root| path.starts_with(root)) {
            violations.push(format!(
                "{} is outside every workspace package",
                display_path(workspace, path)
            ));
        } else if !reachable.contains(path) {
            violations.push(format!(
                "{} is unreachable from every Cargo target root",
                display_path(workspace, path)
            ));
        }
    }
    violations.sort();
    violations.dedup();
    violations
}

struct SourceGraph<'a> {
    workspace: &'a Path,
    package_root: &'a Path,
    reachable: BTreeSet<PathBuf>,
    walked: BTreeSet<(PathBuf, PathBuf)>,
    active: BTreeSet<PathBuf>,
    violations: Vec<String>,
}

impl<'a> SourceGraph<'a> {
    fn new(workspace: &'a Path, package_root: &'a Path) -> Self {
        Self {
            workspace,
            package_root,
            reachable: BTreeSet::new(),
            walked: BTreeSet::new(),
            active: BTreeSet::new(),
            violations: Vec::new(),
        }
    }

    fn walk_file(&mut self, path: &Path, module_dir: &Path) {
        if self.active.contains(path) {
            self.violations.push(format!(
                "{} forms a recursive module path",
                display_path(self.workspace, path)
            ));
            return;
        }
        if !self.bounded(path, "module") {
            return;
        }
        self.reachable.insert(path.to_path_buf());
        let context = (path.to_path_buf(), module_dir.to_path_buf());
        if !self.walked.insert(context) {
            return;
        }
        self.active.insert(path.to_path_buf());
        let source = match fs::read_to_string(path) {
            Ok(source) => source,
            Err(error) => {
                self.violations.push(format!(
                    "{} cannot be read: {error}",
                    display_path(self.workspace, path)
                ));
                self.active.remove(path);
                return;
            }
        };
        match syn::parse_file(&source) {
            Ok(syntax) => {
                let path_base = path.parent().unwrap_or(module_dir);
                self.walk_items(path, module_dir, path_base, &syntax.items);
            }
            Err(error) => self.violations.push(format!(
                "{} does not parse: {error}",
                display_path(self.workspace, path)
            )),
        }
        self.active.remove(path);
    }

    fn walk_items(&mut self, source: &Path, module_dir: &Path, path_base: &Path, items: &[Item]) {
        for item in items {
            let Item::Mod(module) = item else {
                continue;
            };
            let name = module.ident.to_string();
            if let Some((_, nested)) = &module.content {
                let child_dir = module_dir.join(name);
                self.walk_items(source, &child_dir, &child_dir, nested);
                continue;
            }
            let path = match module_path(path_base, module_dir, &name, &module.attrs) {
                Ok(path) => path,
                Err(error) => {
                    self.violations.push(format!(
                        "{} module `{name}` {error}",
                        display_path(self.workspace, source)
                    ));
                    continue;
                }
            };
            let child_dir = module_dir.join(name);
            self.walk_file(&path, &child_dir);
        }
    }

    fn bounded(&mut self, path: &Path, kind: &str) -> bool {
        let Ok(relative) = path.strip_prefix(self.package_root) else {
            self.violations.push(format!(
                "{kind} source {} escapes package {}",
                path.display(),
                self.package_root.display()
            ));
            return false;
        };
        let relative = relative.to_string_lossy().replace('\\', "/");
        if !valid_relative_policy_path(&relative) {
            self.violations.push(format!(
                "{kind} source {} is not a bounded canonical path",
                path.display()
            ));
            return false;
        }
        let canonical_package = self.package_root.canonicalize();
        let canonical_path = path.canonicalize();
        if !matches!(
            (canonical_package, canonical_path),
            (Ok(package), Ok(path)) if path.starts_with(&package)
        ) {
            self.violations.push(format!(
                "{kind} source {} escapes its package after canonicalization",
                path.display()
            ));
            return false;
        }
        true
    }
}

fn module_path(
    path_base: &Path,
    module_dir: &Path,
    name: &str,
    attributes: &[Attribute],
) -> Result<PathBuf, String> {
    let paths = attributes
        .iter()
        .filter_map(direct_path)
        .collect::<Result<Vec<_>, _>>()?;
    if paths.len() > 1 {
        return Err("has multiple #[path] attributes".to_owned());
    }
    if attributes.iter().any(cfg_attr_selects_path) {
        return Err("uses conditional #[path] selection".to_owned());
    }
    if let Some(path) = paths.first() {
        if !valid_relative_policy_path(path) {
            return Err(format!("uses unbounded #[path = {path:?}]"));
        }
        let selected = path_base.join(path);
        return selected
            .is_file()
            .then_some(selected)
            .ok_or_else(|| format!("selects missing source {path:?}"));
    }
    let flat = module_dir.join(format!("{name}.rs"));
    let nested = module_dir.join(name).join("mod.rs");
    match (flat.is_file(), nested.is_file()) {
        (true, false) => Ok(flat),
        (false, true) => Ok(nested),
        (true, true) => Err("has ambiguous flat and nested sources".to_owned()),
        (false, false) => Err("has no source file".to_owned()),
    }
}

fn direct_path(attribute: &Attribute) -> Option<Result<String, String>> {
    let Meta::NameValue(value) = &attribute.meta else {
        return None;
    };
    if !value.path.is_ident("path") {
        return None;
    }
    Some(match &value.value {
        syn::Expr::Lit(expression) => match &expression.lit {
            syn::Lit::Str(path) => Ok(path.value()),
            _ => Err("has a non-string #[path]".to_owned()),
        },
        _ => Err("has a non-literal #[path]".to_owned()),
    })
}

fn cfg_attr_selects_path(attribute: &Attribute) -> bool {
    if !attribute.path().is_ident("cfg_attr") {
        return false;
    }
    match attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) {
        Ok(entries) => entries.iter().skip(1).any(meta_selects_path),
        Err(_) => true,
    }
}

fn meta_selects_path(meta: &Meta) -> bool {
    match meta {
        Meta::NameValue(value) => value.path.is_ident("path"),
        Meta::List(list) if list.path.is_ident("cfg_attr") => {
            match list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) {
                Ok(entries) => entries.iter().skip(1).any(meta_selects_path),
                Err(_) => true,
            }
        }
        Meta::Path(_) | Meta::List(_) => false,
    }
}
