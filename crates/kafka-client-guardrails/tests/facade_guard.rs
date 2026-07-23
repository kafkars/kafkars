//! `lib.rs` and `mod.rs` remain declarative navigation surfaces.

mod support;

use std::path::{Path, PathBuf};

use support::{
    display_path, fixture_files, is_facade, load_config, read, rust_files, workspace_root,
};
use syn::{Item, Visibility};

fn facades_with_implementation(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files.iter().filter(|path| is_facade(path)) {
        let source = read(path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(root, path)));
        for item in syntax.items {
            if !declarative_facade_item(&item) {
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

fn declarative_facade_item(item: &Item) -> bool {
    match item {
        Item::Mod(module) => module.content.is_none(),
        Item::Use(import) => match &import.vis {
            Visibility::Public(_) => true,
            Visibility::Restricted(restricted) => restricted
                .path
                .segments
                .first()
                .is_some_and(|segment| segment.ident == "crate" || segment.ident == "super"),
            Visibility::Inherited => false,
        },
        _ => false,
    }
}

fn item_kind(item: &Item) -> &'static str {
    match item {
        Item::Const(_) => "const",
        Item::Enum(_) => "enum",
        Item::Fn(_) => "function",
        Item::Impl(_) => "impl",
        Item::Macro(_) => "macro",
        Item::Mod(module) if module.content.is_some() => "inline module",
        Item::Mod(_) => "module declaration",
        Item::Static(_) => "static",
        Item::Struct(_) => "struct",
        Item::Trait(_) => "trait",
        Item::Type(_) => "type alias",
        Item::Use(_) => "private import",
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
    assert!(
        violations
            .iter()
            .any(|value| value.contains("inline module")),
        "facade detector accepted an inline module: {violations:?}"
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("private import")),
        "facade detector accepted a private import: {violations:?}"
    );
}
