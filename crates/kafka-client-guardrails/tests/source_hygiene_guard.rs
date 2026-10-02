//! Production paths keep tests separate; client-owning examples observe shutdown.

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

fn example_binding(local: &syn::Local) -> Option<(&syn::Ident, &syn::Expr)> {
    let pattern = match &local.pat {
        syn::Pat::Type(typed) => typed.pat.as_ref(),
        pattern => pattern,
    };
    let syn::Pat::Ident(binding) = pattern else {
        return None;
    };
    local
        .init
        .as_ref()
        .map(|initialization| (&binding.ident, initialization.expr.as_ref()))
}

fn example_observes_shutdown(function: &ItemFn) -> bool {
    use syn::{Expr, Stmt};

    let [
        Stmt::Local(client),
        Stmt::Local(result),
        Stmt::Local(shutdown),
        Stmt::Expr(Expr::MethodCall(combine), None),
    ] = function.block.stmts.as_slice()
    else {
        return false;
    };
    let (Some((client_name, _)), Some((result_name, result)), Some((shutdown_name, shutdown))) = (
        example_binding(client),
        example_binding(result),
        example_binding(shutdown),
    ) else {
        return false;
    };
    if !matches!(result, Expr::Await(awaited) if matches!(awaited.base.as_ref(), Expr::Async(_))) {
        return false;
    }
    let shutdown = match shutdown {
        Expr::MethodCall(mapping) if mapping.method == "map_err" => mapping.receiver.as_ref(),
        expression => expression,
    };
    let Expr::Await(awaited) = shutdown else {
        return false;
    };
    let Expr::MethodCall(shutdown) = awaited.base.as_ref() else {
        return false;
    };
    shutdown.method == "shutdown"
        && shutdown.args.is_empty()
        && matches!(shutdown.receiver.as_ref(), Expr::Path(path) if path.path.is_ident(client_name))
        && combine.method == "and"
        && matches!(combine.receiver.as_ref(), Expr::Path(path) if path.path.is_ident(result_name))
        && combine.args.len() == 1
        && matches!(&combine.args[0], Expr::Path(path) if path.path.is_ident(shutdown_name))
}

#[test]
fn client_owning_examples_observe_shutdown_without_replacing_operation_errors() {
    let examples = workspace_root().join("crates/kafkars/examples");
    let mut violations = Vec::new();
    for (file, functions) in [
        ("producer.rs", &["produce"][..]),
        ("consumer.rs", &["consume"][..]),
        ("transaction.rs", &["initialize_transactional_owner"][..]),
        (
            "admin.rs",
            &[
                "create_topic",
                "delete_topics",
                "create_partitions",
                "list_visible_topics",
            ][..],
        ),
    ] {
        let syntax = syn::parse_file(&read(&examples.join(file)))
            .unwrap_or_else(|error| panic!("parse {file}: {error}"));
        for name in functions {
            let function = syntax
                .items
                .iter()
                .find_map(|item| match item {
                    syn::Item::Fn(function) if function.sig.ident == name => Some(function),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("missing {file}::{name}"));
            if !example_observes_shutdown(function) {
                violations.push(format!("{file}::{name}"));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "examples bypass terminal shutdown: {violations:?}"
    );
}

#[test]
fn example_shutdown_guard_rejects_early_error_escape_and_error_replacement() {
    for source in [
        "async fn run() -> Result<()> {
            let client = Client::builder().build()?;
            let producer = client.producer().build()?;
            let result = async { producer.send().await }.await;
            let shutdown = client.shutdown().await;
            result.and(shutdown)
        }",
        "async fn run() -> Result<()> {
            let client = Client::builder().build()?;
            let result = async { operation().await }.await;
            let shutdown = client.shutdown().await;
            shutdown.and(result)
        }",
    ] {
        let function = syn::parse_str::<ItemFn>(source)
            .unwrap_or_else(|error| panic!("parse refusal example: {error}"));
        assert!(!example_observes_shutdown(&function));
    }
}
