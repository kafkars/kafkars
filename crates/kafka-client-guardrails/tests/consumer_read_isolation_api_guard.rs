//! Exact closed-value and sibling-test ratchets for consumer read-isolation configuration.

mod support;

use std::path::Path;

use support::{load_config, read, workspace_root};

const FACADE: &str = "crates/kafka-client/src/consumer/read_isolation.rs";
const FACADE_TEST: &str = "crates/kafka-client/src/consumer/read_isolation_test.rs";
const ENGINE: &str = "crates/kafka-client-engine/src/config/read_isolation.rs";
const ENGINE_TEST: &str = "crates/kafka-client-engine/src/config/read_isolation_test.rs";
const EXPECTED: [&str; 2] = ["ReadUncommitted", "ReadCommitted"];

#[test]
fn checked_in_read_isolation_values_and_sibling_tests_are_exact() {
    let root = workspace_root();
    assert_eq!(
        enum_variants(&root.join(FACADE), "ReadIsolation"),
        expected_variants()
    );
    assert_eq!(
        enum_variants(&root.join(ENGINE), "ConsumerReadIsolation"),
        expected_variants()
    );

    let config = load_config(&root);
    for (production, test) in [(FACADE, FACADE_TEST), (ENGINE, ENGINE_TEST)] {
        let mirrors = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(mirrors.len(), 1, "{production} needs one test mirror");
        assert_eq!(mirrors[0].test, test);
    }
}

#[test]
fn fixture_rejects_unreviewed_public_and_engine_variants() {
    let root = workspace_root()
        .join("crates/kafka-client-guardrails/tests/fixtures/consumer_read_isolation");
    let path = root.join("src/read_isolation.rs");

    assert_ne!(enum_variants(&path, "ReadIsolation"), expected_variants());
    assert_ne!(
        enum_variants(&path, "ConsumerReadIsolation"),
        expected_variants()
    );
}

fn expected_variants() -> Vec<String> {
    EXPECTED
        .iter()
        .map(|variant| (*variant).to_owned())
        .collect()
}

fn enum_variants(path: &Path, enum_name: &str) -> Vec<String> {
    let source = read(path);
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(item) if item.ident == enum_name => Some(
                item.variants
                    .iter()
                    .map(|variant| variant.ident.to_string())
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{} lacks enum {enum_name}", path.display()))
}
