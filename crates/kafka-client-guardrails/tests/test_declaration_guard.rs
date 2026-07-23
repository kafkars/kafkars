//! Every sibling `*_test.rs` unit test is declared and compiled.

mod support;

use std::path::{Path, PathBuf};

use support::{display_path, fixture_files, load_config, read, rust_files, workspace_root};
use syn::{Attribute, Expr, Item, Lit, Meta};

fn undeclared_tests(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files.iter().filter(|path| is_unit_test(path)) {
        let relative = display_path(root, path);
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(facade) = sibling_facade(path) else {
            violations.push(format!(
                "{relative} has no sibling facade and runs zero tests"
            ));
            continue;
        };
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        match declaration(&read(&facade), stem, file_name) {
            Declaration::Gated => {}
            Declaration::Ungated => violations.push(format!(
                "{} declares `{stem}` without #[cfg(test)]",
                display_path(root, &facade)
            )),
            Declaration::Redirected => violations.push(format!(
                "{} redirects `{stem}` away from sibling test {relative}",
                display_path(root, &facade)
            )),
            Declaration::Absent => {
                violations.push(format!("{relative} is undeclared and runs zero tests"));
            }
        }
    }
    violations
}

fn is_unit_test(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.ends_with("_test.rs"))
        && path.components().any(|part| part.as_os_str() == "src")
}

fn sibling_facade(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    ["mod.rs", "lib.rs", "main.rs"]
        .iter()
        .map(|name| parent.join(name))
        .find(|candidate| candidate.is_file())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Declaration {
    Gated,
    Ungated,
    Redirected,
    Absent,
}

fn declaration(source: &str, stem: &str, file_name: &str) -> Declaration {
    let Ok(syntax) = syn::parse_file(source) else {
        return Declaration::Absent;
    };
    let mut redirected = false;
    for item in syntax.items {
        let Item::Mod(module) = item else {
            continue;
        };
        if module.content.is_some() {
            continue;
        }
        let explicit_path = module.attrs.iter().find_map(module_path);
        let names_sibling = module.ident == stem && explicit_path.is_none();
        let points_to_sibling = explicit_path.as_deref() == Some(file_name);
        if module.ident == stem && explicit_path.is_some() && !points_to_sibling {
            redirected = true;
        }
        if !names_sibling && !points_to_sibling {
            continue;
        }
        return if module.attrs.iter().any(is_cfg_test) {
            Declaration::Gated
        } else {
            Declaration::Ungated
        };
    }
    if redirected {
        Declaration::Redirected
    } else {
        Declaration::Absent
    }
}

fn module_path(attribute: &Attribute) -> Option<String> {
    let Meta::NameValue(name_value) = &attribute.meta else {
        return None;
    };
    if !name_value.path.is_ident("path") {
        return None;
    }
    let Expr::Lit(expression) = &name_value.value else {
        return None;
    };
    let Lit::Str(value) = &expression.lit else {
        return None;
    };
    Some(value.value())
}

fn is_cfg_test(attribute: &Attribute) -> bool {
    attribute.path().is_ident("cfg")
        && attribute
            .parse_args::<syn::Path>()
            .is_ok_and(|path| path.is_ident("test"))
}

#[test]
fn every_live_sibling_unit_test_is_declared_and_gated() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = undeclared_tests(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "unit-test declaration violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn undeclared_and_ungated_tests_are_rejected() {
    let (root, files) = fixture_files("undeclared_unit_test");
    let violations = undeclared_tests(&root, &files);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("orphan_test.rs"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("without #[cfg(test)]"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("redirects `redirected_test`"))
    );
}
