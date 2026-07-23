//! `lib.rs` and `mod.rs` remain declarative navigation surfaces.

mod support;

use std::path::{Path, PathBuf};

use support::{
    display_path, fixture_files, is_facade, load_config, read, rust_files, workspace_root,
};
use syn::Item;

fn facades_with_implementation(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files.iter().filter(|path| is_facade(path)) {
        let source = read(path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, path)));
        for item in syntax.items {
            if !matches!(item, Item::Mod(_) | Item::Use(_)) {
                violations.push(format!(
                    "{} contains implementation item {}",
                    display_path(root, path),
                    item_kind(&item)
                ));
            }
        }
    }
    violations
}

fn item_kind(item: &Item) -> &'static str {
    match item {
        Item::Const(_) => "const",
        Item::Enum(_) => "enum",
        Item::Fn(_) => "function",
        Item::Impl(_) => "impl",
        Item::Macro(_) => "macro",
        Item::Static(_) => "static",
        Item::Struct(_) => "struct",
        Item::Trait(_) => "trait",
        Item::Type(_) => "type alias",
        _ => "unsupported item",
    }
}

#[test]
fn live_facades_only_declare_modules_and_reexports() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = facades_with_implementation(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "facade architecture violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn a_facade_carrying_a_function_is_rejected() {
    let (root, files) = fixture_files("facade_with_implementation");
    let violations = facades_with_implementation(&root, &files);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("lib.rs") && value.contains("function")),
        "facade detector accepted implementation: {violations:?}"
    );
}
