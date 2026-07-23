//! Production reading paths exclude embedded tests and unfinished escape hatches.

mod support;

use std::path::{Path, PathBuf};

use support::{
    display_path, fixture_files, is_integration_test, load_config, read, rust_files,
    workspace_package_roots, workspace_root,
};
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{Attribute, ItemFn, ItemMod, Meta, Token};

fn source_hygiene_violations(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut violations = Vec::new();
    let package_roots = workspace_package_roots(root);
    for path in files {
        let relative = display_path(root, path);
        if is_integration_test(&package_roots, path) || relative.ends_with("_test.rs") {
            continue;
        }
        let source = read(path);
        for forbidden in ["todo!", "unimplemented!", "dbg!"] {
            if source.contains(forbidden) {
                violations.push(format!("{relative} contains forbidden `{forbidden}`"));
            }
        }
        let syntax =
            syn::parse_file(&source).unwrap_or_else(|error| panic!("parse {relative}: {error}"));
        let mut collector = InlineTestCollector::default();
        collector.visit_file(&syntax);
        if collector.has_test_function {
            violations.push(format!(
                "{relative} embeds a test function; move it to a sibling `*_test.rs` file"
            ));
        }
        if collector.has_inline_test_module {
            violations.push(format!("{relative} embeds an inline test module"));
        }
    }
    violations
}

#[derive(Default)]
struct InlineTestCollector {
    has_test_function: bool,
    has_inline_test_module: bool,
}

impl<'ast> Visit<'ast> for InlineTestCollector {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        if function.attrs.iter().any(attribute_marks_test_item) {
            self.has_test_function = true;
        }
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_item_mod(&mut self, module: &'ast ItemMod) {
        let test_name = {
            let name = module.ident.to_string();
            name == "tests" || name.ends_with("_test")
        };
        if module.content.is_some() && (test_name || module.attrs.iter().any(attribute_gates_test))
        {
            self.has_inline_test_module = true;
        }
        syn::visit::visit_item_mod(self, module);
    }
}

fn attribute_marks_test_item(attribute: &Attribute) -> bool {
    attribute.path().is_ident("test") || attribute_gates_test(attribute)
}

fn attribute_gates_test(attribute: &Attribute) -> bool {
    if attribute.path().is_ident("cfg") {
        return attribute
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .is_ok_and(|nested| nested.iter().any(meta_mentions_test));
    }
    if !attribute.path().is_ident("cfg_attr") {
        return false;
    }
    attribute
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .is_ok_and(|nested| nested.iter().skip(1).any(meta_marks_test_item))
}

fn meta_marks_test_item(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("cfg") => list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .is_ok_and(|nested| nested.iter().any(meta_mentions_test)),
        Meta::List(list) if list.path.is_ident("cfg_attr") => list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .is_ok_and(|nested| nested.iter().skip(1).any(meta_marks_test_item)),
        Meta::List(_) | Meta::NameValue(_) => false,
    }
}

fn meta_mentions_test(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) => {
            if list.path.is_ident("test") {
                return true;
            }
            list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .is_ok_and(|nested| nested.iter().any(meta_mentions_test))
        }
        Meta::NameValue(_) => false,
    }
}

#[test]
fn production_sources_are_finished_and_keep_tests_separate() {
    let workspace = workspace_root();
    let config = load_config(&workspace);
    let violations = source_hygiene_violations(&workspace, &rust_files(&workspace, &config));

    assert!(
        violations.is_empty(),
        "source hygiene violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn embedded_tests_and_placeholders_are_rejected() {
    let (root, files) = fixture_files("inline_test_body");
    let violations = source_hygiene_violations(&root, &files);

    for file in ["lib.rs", "formatted.rs", "qualified.rs"] {
        assert!(
            violations.iter().any(|value| value.contains(file)),
            "AST test detector accepted {file}: {violations:?}"
        );
    }
    assert!(
        violations
            .iter()
            .any(|value| value.contains("test function"))
    );
    assert!(violations.iter().any(|value| value.contains("todo!")));

    let (root, files) = fixture_files("nested_test_directory");
    let violations = source_hygiene_violations(&root, &files);
    assert!(
        violations
            .iter()
            .any(|value| value.contains("owner/tests/case.rs")),
        "nested src tests directory bypassed hygiene: {violations:?}"
    );
}
