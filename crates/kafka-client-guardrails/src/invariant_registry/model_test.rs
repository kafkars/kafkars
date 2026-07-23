//! Negative schema fixtures for invariant registry parsing.

use super::model;

const VALID: &str = r#"
schema = 1

[[invariant]]
id = "AA-01"
title = "Actual test"
statement = "The evidence is executable."
status = "enforced"
evidence = ["src/evidence_test.rs::actual_test"]
"#;

#[test]
fn strict_schema_accepts_only_declared_registry_and_entry_fields() {
    assert!(model::parse(VALID).is_ok());
    assert!(model::parse(&VALID.replacen("schema = 1", "schema = 1\nextra = 2", 1)).is_err());
    assert!(model::parse(&VALID.replacen("title =", "unknown = \"x\"\ntitle =", 1)).is_err());
    assert!(model::parse(&VALID.replace("statement = ", "missing_statement = ")).is_err());
}
