//! Exact source-file and Rust test-item evidence resolution.

use std::fs;
use std::path::{Component, Path};

use syn::Item;

pub(super) fn violations(workspace: &Path, reference: &str) -> Vec<String> {
    let Some((relative, test_name)) = reference.rsplit_once("::") else {
        return vec![format!(
            "evidence `{reference}` must be `relative/path_test.rs::test_name`"
        )];
    };
    if !canonical_path(relative) || test_name.is_empty() {
        return vec![format!("evidence `{reference}` is not canonical")];
    }
    if !relative.ends_with("_test.rs") {
        return vec![format!(
            "evidence `{reference}` does not name a sibling `*_test.rs` module"
        )];
    }

    let path = workspace.join(relative);
    let source = match fs::read_to_string(&path) {
        Ok(source) => source,
        Err(error) => {
            return vec![format!("evidence `{reference}` cannot be read: {error}")];
        }
    };
    let syntax = match syn::parse_file(&source) {
        Ok(syntax) => syntax,
        Err(error) => {
            return vec![format!("evidence `{reference}` is invalid Rust: {error}")];
        }
    };
    let matches = syntax
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Fn(function) if function.sig.ident == test_name => Some(function),
            _ => None,
        })
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return vec![format!("evidence `{reference}` names no function")];
    }
    if matches.len() != 1 {
        return vec![format!("evidence `{reference}` is ambiguous")];
    }
    if !matches[0]
        .attrs
        .iter()
        .any(|attribute| attribute.path().is_ident("test"))
    {
        return vec![format!(
            "evidence `{reference}` names an ordinary function, not an actual `#[test]`"
        )];
    }
    Vec::new()
}

fn canonical_path(value: &str) -> bool {
    if value.is_empty() || value.contains('\\') {
        return false;
    }
    let normalized = Path::new(value)
        .components()
        .map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()
        .map(|components| components.join("/"));
    normalized.as_deref() == Some(value)
}
