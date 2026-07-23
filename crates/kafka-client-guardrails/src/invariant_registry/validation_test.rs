//! Registry status, identity, uniqueness, and liveness detector fixtures.

use std::path::{Path, PathBuf};

use super::{model, validation};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/invariant_registry")
}

fn validate(source: &str) -> Vec<String> {
    let registry = model::parse(source).unwrap_or_else(|error| panic!("fixture parses: {error}"));
    validation::validate(&fixture(), &registry)
}

#[test]
fn valid_enforced_and_planned_entries_are_accepted() {
    let violations = validate(
        r#"
schema = 1
[[invariant]]
id = "AA-01"
title = "Exact evidence"
statement = "One actual Rust test supports this claim."
status = "enforced"
evidence = ["src/evidence_test.rs::actual_test"]
[[invariant]]
id = "BB-01"
title = "Named future"
statement = "This claim has not landed yet."
status = "planned"
milestone = "fixture-next-milestone"
"#,
    );
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn empty_unknown_and_malformed_registry_entries_are_rejected() {
    let empty = validate("schema = 1\ninvariant = []\n");
    assert!(empty.iter().any(|value| value.contains("is empty")));

    let violations = validate(
        r#"
schema = 2
[[invariant]]
id = "bad"
title = ""
statement = """
line one
line two"""
status = "unknown"
"#,
    );
    for expected in [
        "unsupported schema",
        "invalid invariant id",
        "invalid title",
        "invalid statement",
        "invalid status",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(expected)),
            "missing `{expected}` violation: {violations:?}"
        );
    }
}

#[test]
fn statuses_cannot_claim_the_wrong_kind_of_progress() {
    let violations = validate(
        r#"
schema = 1
[[invariant]]
id = "AA-01"
title = "Enforced without a test"
statement = "This has no executable evidence."
status = "enforced"
milestone = "already-enforced"
[[invariant]]
id = "BB-01"
title = "Planned with stale evidence"
statement = "This improperly claims a test."
status = "planned"
evidence = ["src/evidence_test.rs::actual_test"]
"#,
    );
    for expected in [
        "AA-01 has no enforced evidence",
        "AA-01 is enforced but names a milestone",
        "BB-01 is planned but claims evidence",
        "BB-01 has no concrete milestone",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(expected)),
            "missing `{expected}` violation: {violations:?}"
        );
    }
}

#[test]
fn duplicate_ids_evidence_and_non_ordered_entries_are_rejected() {
    let violations = validate(
        r#"
schema = 1
[[invariant]]
id = "BB-01"
title = "First claim"
statement = "The first claim has evidence."
status = "enforced"
evidence = ["src/evidence_test.rs::actual_test"]
[[invariant]]
id = "BB-01"
title = "Duplicate claim"
statement = "The duplicate claim reuses evidence."
status = "enforced"
evidence = ["src/evidence_test.rs::actual_test"]
"#,
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("duplicate invariant id"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("claimed more than once"))
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("not strictly ordered"))
    );
}

#[test]
fn stale_evidence_and_missing_registries_are_rejected() {
    let stale = validate(
        r#"
schema = 1
[[invariant]]
id = "AA-01"
title = "Stale evidence"
statement = "The test no longer exists."
status = "enforced"
evidence = ["src/evidence_test.rs::renamed_test"]
"#,
    );
    assert!(
        stale
            .iter()
            .any(|value| value.contains("names no function"))
    );

    let missing_registry = validation::check_invariant_registry(&fixture().join("missing"));
    assert!(
        missing_registry
            .iter()
            .any(|value| value.contains("read contracts/invariants.toml"))
    );
}
